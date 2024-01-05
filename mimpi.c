#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct sendData {
    int count;
    int tag;
    const void *data;
} sendData;

/// LINKED LIST IMPLEMENTATION ///
// Struktura węzła listy
typedef struct Node {
    sendData *data;
    struct Node *next;
    struct Node *prev;
} Node;

// Struktura listy
typedef struct LinkedList {
    Node *head;
    Node *tail;
} LinkedList;

// Funkcja inicjalizująca pustą listę
LinkedList *createLinkedList() {
    LinkedList *list = (LinkedList *)malloc(sizeof(LinkedList));
    ASSERT_MALLOC_OK(list);
    list->head = NULL;
    list->tail = NULL;
    return list;
}

// Funkcja dodająca węzeł na koniec listy
void append(LinkedList *list, sendData *data) {
    Node *newNode = (Node *)malloc(sizeof(Node));
    ASSERT_MALLOC_OK(newNode);
    newNode->data = data;
    newNode->prev = NULL;
    newNode->next = NULL;

    if (list->head == NULL) {
        list->head = newNode;
    } else {
        list->tail->next = newNode;
        newNode->prev = list->tail;
    }
    list->tail = newNode;
}

// Funkcja usuwająca węzeł na podstawie wskazanego węzła. Zwalnia pamięc na
// data.
// TODO: ta funkcja moze byc calkiem niepotrzebna?
void deleteNodeFreeData(LinkedList *list, Node *nodeToDelete) {
    if (nodeToDelete->prev != NULL) {
        nodeToDelete->prev->next = nodeToDelete->next;
    } else {
        list->head = nodeToDelete->next;
    }

    if (nodeToDelete->next != NULL) {
        nodeToDelete->next->prev = nodeToDelete->prev;
    } else {
        list->tail = nodeToDelete->prev;
    }

    free(nodeToDelete->data);
    free(nodeToDelete);
}

// Funkcja usuwająca węzeł na podstawie wskazanego węzła. Zwalnia pamięc na
// data.
void deleteNode(LinkedList *list, Node *nodeToDelete) {
    if (nodeToDelete->prev != NULL) {
        nodeToDelete->prev->next = nodeToDelete->next;
    } else {
        list->head = nodeToDelete->next;
    }

    if (nodeToDelete->next != NULL) {
        nodeToDelete->next->prev = nodeToDelete->prev;
    } else {
        list->tail = nodeToDelete->prev;
    }

    free(nodeToDelete);
}

// Funkcja dodająca węzeł po wskazanym węźle
void insertAfterNode(LinkedList *list, Node *prevNode, sendData *data) {
    if (prevNode == NULL) {
        printf("Previous node cannot be NULL\n");
        return;
    }

    Node *newNode = (Node *)malloc(sizeof(Node));
    ASSERT_MALLOC_OK(newNode);
    newNode->data = data;

    if (prevNode->next != NULL) {
        prevNode->next->prev = newNode;
    } else {
        list->tail = newNode;
    }

    newNode->next = prevNode->next;
    prevNode->next = newNode;
    newNode->prev = prevNode;
}

// Funkcja wyświetlająca zawartość listy
void displayList(LinkedList *list) {
    Node *temp = list->head;
    while (temp != NULL) {
        printf("(%d, %d) -> ", temp->data->tag, temp->data->count);
        temp = temp->next;
    }
    printf("NULL\n");
}

// Funkcja usuwająca wszystkie węzły z listy. Zwalnia pamieć która zajmuje data
void deleteLinkedList(LinkedList *list) {
    Node *current = list->head;
    Node *next;

    while (current != NULL) {
        next = current->next;
        free(current->data);
        free(current);
        current = next;
    }

    free(list);
}

sendData *searchAndRemoveMessage(LinkedList *list, int tag, int count) {
    Node *curr = list->head;

    while (curr != list->tail &&
           (tag == MIMPI_ANY_TAG && curr->data->count != count) &&
           (curr->data->tag != tag || curr->data->count != count)) {
        curr = curr->next;
    }

    if (curr == list->tail) { // Nie ma szukanej wiadomości
        return NULL;
    }

    sendData *data = curr->data;
    deleteNode(list, curr);
    return data;
}


/// LINKED LIST IMPLEMENTATION END ///

/// GLOBALS ///
// Oznacza, ze proces nie czeka na odebranie wiadomosci.
// Tego tagu nie można używać żeby wysyłać pomocnicze wiadomości
// do procesów.
#define NOWAIT_TAG -1

static bool DEADLOCK_DETECTION;

static int WORLD_SIZE;
static int WORLD_RANK;
// Na indeksie i jest numer deskryptora do czytania wiadomości od i-tego procesu
int writeDescriptors[MAX_PROCESSES + 1];
// Na indeksie i jest numer deskryptora do pisania do i-tego procesu
int readDescriptors[MAX_PROCESSES + 1];

// Helper thread data
pthread_t helperThread;
bool helperThreadActive = true;
fd_set readDescriptorsSet;
int maxFd = 0; // max file descriptor(for select purposes)

pthread_mutex_t mainThreadMutex;
int tagIWaitFor = NOWAIT_TAG;
int countIWaitFor = NOWAIT_TAG;
int sourceIWaitFor = NOWAIT_TAG;
// Jeżeli helper odczytał dane, na które akurat czeka recv to nie wrzuca ich bez
// sensu do listy żeby uniknać podwójnego szukania
sendData *justRecieved = NULL;

// Tablica linked list. Na i-tym indeksie jest lista wiadomości przychodzących
// od i-tego procesu
LinkedList *messageBuffer[MAX_PROCESSES];
/// GLOBALS END ///

void printBuffer(LinkedList *messageBuffer[MAX_PROCESSES]) {
    printf("======================BUFFER======================\n");
    for (int i = 0; i < WORLD_SIZE; i++) {
        printf("%d:   ", i);
        displayList(messageBuffer[i]);
    }
    printf("==================================================\n");
}


void *helperThreadMain() {
    // Dodajemy wszystkie desktyptory do zbioru śledzonych
    printf("WELCOME FROM %d HELPER THREAD!\n", WORLD_RANK);

    FD_ZERO(&readDescriptorsSet);
    for (int i = 0; i < WORLD_SIZE; i++) {
        printf("%d", i);
        FD_SET(readDescriptors[i], &readDescriptorsSet);
    }
    printf("\n HELPER: added desctiprors");

    int startingIndex = 0, i = 0, count = 0;
    while (helperThreadActive) {
        printf("HELPER: New while rotation: %d, %d\n", startingIndex, i);
        // Jeżeli nie ma żadnych gotowych desktyptorów to wątek czeka
        int ready = select(maxFd + 1, &readDescriptorsSet, NULL, NULL, NULL);
        ASSERT_SYS_OK(ready);
        printf("HELPER: Woke up form select, this many wait: %d\n", ready);

        // Przechodzimy do deskryptorach cyklicznie, zaczynajac zawsze od
        // różnych żeby uniknąć zagłodzenia dużych dekstyptorów
        i = startingIndex;
        count = 0;

        // TODO: Co jeżeli na jednym channelu czeka kilka wiadomości. Chyba to
        // już rozwiązane
        while (count < WORLD_SIZE) {
            printf("==========================================\n");
            printf("HELPER: Looking for ready pipes: i: %d, count: %d\n", i,
                   count);
            if (FD_ISSET(readDescriptors[i], &readDescriptorsSet)) {
                printf("HELPER: Someone is waiting here\n");
                // Wczytujemy tylko rozmiar danych, zeby wiedziec ile pamięci
                // zaalokować.
                // Ten while tutaj to troche z armaty do wróbla.
                // TODO: Może usuń while'a.
                int count;
                int totalRead = 0;
                int totalToBeRead = sizeof(int);
                while (totalRead < totalToBeRead) {
                    ssize_t read =
                        chrecv(readDescriptors[i], &count + totalRead, totalToBeRead);
                    ASSERT_SYS_OK(read);
                    totalRead += read;
                    totalToBeRead -= read;
                }
                printf("HELPER: Read count: %d\n", count);
                sendData *recievedData =
                    (sendData *)malloc(2 * sizeof(int) + count);
                ASSERT_MALLOC_OK(recievedData);
                recievedData->count = count;

                // Czytamy tag i dane wielkosci count, count juz wczytany
                totalRead = 0;
                totalToBeRead = sizeof(int) + recievedData->count;
                while (totalRead < totalToBeRead) {
                    ssize_t read = chrecv(
                        readDescriptors[i],
                        recievedData + sizeof(int) + totalRead, totalToBeRead);
                    ASSERT_SYS_OK(read);
                    totalRead += read;
                    totalToBeRead -= read;
                }

                printf("HELPER: Read all the data\n");
                // Tutaj recievedData powinno być już wypełnione. Można je dodać
                // do naszego bufora

                // Sprawdzamy czy ktoś czeka na to co własnie wczytaliśmy
                if (countIWaitFor == recievedData->count &&
                    sourceIWaitFor == i &&
                    (tagIWaitFor == recievedData->tag ||
                     tagIWaitFor == MIMPI_ANY_TAG)) {
                    printf(
                        "HELPER: Someone was waiting for that exact message\n");
                    justRecieved = recievedData;
                    ASSERT_ZERO(pthread_mutex_unlock(&mainThreadMutex));
                } else {
                    printf("HELPER: No one is waiting, appending to buffer\n");
                    append(messageBuffer[i], recievedData);
                }
            }
            i = (i + 1) % WORLD_SIZE;
            count++;
        }
        startingIndex = (startingIndex + 1) % WORLD_SIZE;
    }
    return NULL;
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    DEADLOCK_DETECTION = enable_deadlock_detection;
    if (enable_deadlock_detection && WORLD_RANK == 0) {
        //  TODO: trzeba stworzyc jakis dodatkowy proces ktory bedzie przyjmował
        //  informacje o wysyłkach i przyjeciach i naklepac do tego jakis graf
    }
    // Inicjalizujemy World Size i World Rank
    const char *world_size_env = getenv("MIMPI_WORLD_SIZE");
    const char *world_rank_env = getenv("MIMPI_WORLD_RANK");
    // TODO: Usun te mallocki
    ASSERT_MALLOC_OK(world_size_env);
    ASSERT_MALLOC_OK(world_rank_env);
    if (world_rank_env != NULL && world_size_env != NULL) {
        WORLD_SIZE = atoi(world_size_env);
        WORLD_RANK = atoi(world_rank_env);
    } else {
        fprintf(stderr, "MIMPI_WORLD_SIZE or MIMPI_WORLD_RANK not set!\n");
        exit(EXIT_FAILURE);
    }

    // Inicjalizujemy tablice deskryptorów
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (i != WORLD_RANK) {
            char write_descriptor[32];
            snprintf(write_descriptor, sizeof(write_descriptor),
                     "MIMPI_CHANNEL_WRITE_%d", i);
            char read_descriptor[32];
            snprintf(read_descriptor, sizeof(read_descriptor),
                     "MIMPI_CHANNEL_READ_%d", i);

            char *temp1 = getenv(write_descriptor);
            char *temp2 = getenv(read_descriptor);

            if (temp1 && temp2) {
                int writeDesc = atoi(temp1);
                int readDesc = atoi(temp2);

                // Maksymalna wartość deskryptora do select
                if (readDesc > maxFd) {
                    maxFd = readDesc;
                }

                writeDescriptors[i] = writeDesc;
                readDescriptors[i] = readDesc;
                if (WORLD_RANK == 1)
                    printf("Ith: %d, %d\n", writeDescriptors[i],
                           readDescriptors[i]);
            }
        }
    }

    // Inicjalizujemy bufor na senda.
    for (int i = 0; i < MAX_PROCESSES; i++) {
        messageBuffer[i] = createLinkedList();
    }

    // Tworzymy wątek pomocniczy
    ASSERT_ZERO(pthread_mutex_init(&mainThreadMutex, NULL));
    ASSERT_ZERO(pthread_create(&helperThread, NULL, helperThreadMain, NULL));
}

void MIMPI_Finalize() {

    // TODO: Musimy wysłać wiadomość do wszystkich procesów które czekały na
    // wiadomość od nas, ze wychodzimy z bloku. Chyba właśnie nie wysyłamy,
    // tylko czekamy az read zwróci zero

    // Ubijamy nasz helper thread
    helperThreadActive = false;
    ASSERT_ZERO(pthread_join(helperThread, NULL));
    ASSERT_ZERO(pthread_mutex_destroy(&mainThreadMutex));

    // Niszczymy bufor na senda.
    for (int i = 0; i < MAX_PROCESSES; i++) {
        deleteLinkedList(messageBuffer[i]);
    }

    // Zamykamy nie potrzebne deskrytptory
    for (int i = 0; i <= WORLD_SIZE; i++) {
        if (i != WORLD_RANK) {
            close(writeDescriptors[i]);
            close(readDescriptors[i]);
        }
    }

    unsetenv("MIMPI_WORLD_SIZE");
    unsetenv("MIMPI_WORLD_RANK");

    // Zwalniamy zmienne środowiskowe do przekazania deskryptorów
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (i != WORLD_RANK) {
            char write_descriptor[32];
            snprintf(write_descriptor, sizeof(write_descriptor),
                     "MIMPI_CHANNEL_WRITE_%d_%d", WORLD_RANK, i);
            char read_descriptor[32];
            snprintf(read_descriptor, sizeof(read_descriptor),
                     "MIMPI_CHANNEL_READ_%d_%d", WORLD_RANK, i);
            unsetenv(write_descriptor);
            unsetenv(read_descriptor);
        }
    }

    channels_finalize();
}

int MIMPI_World_size() { return WORLD_SIZE; }

int MIMPI_World_rank() { return WORLD_RANK; }

MIMPI_Retcode MIMPI_Send(void const *data, int count, int destination,
                         int tag) {

    // Jezeli wchodzimy tu i chcemy wysłać wiadomość do procesu do destination
    // ktory juz nie istnieje bo wyszedl z bloku MPI to zwracamy
    // MIMPI_ERROR_REMOTE_FINISHED Chyba tu nie zwracamy MIMPI_DEADLOCK_DETECTED

    // TODO: Sprawdzenie czy program do ktorego wysyłamy wyszedł z bloku MIMPI
    printf("MAIN %d: Entering MIMPI_Send\n", WORLD_RANK);
    if (destination < 0 || destination > WORLD_SIZE - 1) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    } else if (destination == WORLD_RANK) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    } else {
        printf("MAIN %d: Everything okey with the message, sending\n", WORLD_RANK);

        // TODO: Tu moze malloc, chyba w sumie nie bo wysyłamy dane sendem
        // przed zniknieciem
        sendData dataStruct;
        dataStruct.count = count;
        dataStruct.tag = tag;
        dataStruct.data = data;

        int totalToBeSent = count + 2 * sizeof(int);
        ssize_t totalSent = 0;
        while (totalSent < count) {
            ssize_t wrote = chsend(writeDescriptors[destination],
                                   &dataStruct + totalSent, totalToBeSent);
            ASSERT_SYS_OK(wrote);
            totalSent += wrote;
            totalToBeSent -= wrote;
        }
        printf("MAIN %d: Sent all data\n", WORLD_RANK);
        return MIMPI_SUCCESS;
    }
}

MIMPI_Retcode MIMPI_Recv(void *data, int count, int source, int tag) {
    // Czyka na wiadomość o wielkosci count i znaczniku tag od procesu o randze
    // rank(zapisanej w source)
    // Zapisuje ta wiadomość pod adresem data, nie musimy martwic się ze cos tam
    // nadpiszemy, bo to wołający ma to ogarnac

    // TODO: Sprawdzenie czy program na ktory czekamy wyszedł z bloku MIMPI i
    // nie wysłał pasującej wiadomosci
    // TODO: Jeżeli proces tu wejdzie i czeka na wiadomość, a w między czasie
    // ten na ktory czekał wyjdzie to wychodzac musi wysłac ping do procesow
    // ktore czekały na wiadomosc od niego.
    printf("MAIN %d: Entered MIMPI_Recv\n", WORLD_RANK);
    if (source < 0 || source > WORLD_SIZE - 1) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    } else if (source == WORLD_RANK) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    // else if (wysylajacy się skończyl i nie wysłał pasujacej wiadomosci) {}

    else {
        printf("MAIN %d: Everything okay with the message, recieving\n", WORLD_RANK);
        sendData *found =
            searchAndRemoveMessage(messageBuffer[source], tag, count);
        if (!found) { // Jeszcze nie ma wiadomości dla nas. Wieszamy się na
                      // mutexie
            printf("MAIN %d: No message was found, I hang myself\n", WORLD_RANK);    
            tagIWaitFor = tag;
            countIWaitFor = count;
            sourceIWaitFor = source;
            ASSERT_ZERO(pthread_mutex_lock(&mainThreadMutex));
            printf("MAIN %d: Woke up from hanging\n", WORLD_RANK);    

            // Jak się tu obudziliśmy to na pewno jest dla nas wiadomość.
            // Oznaczmy ze nikt nie czeka, zeby pomocniczy wiedział i nie musiał
            // sprawdzac mutexa
            tagIWaitFor = NOWAIT_TAG;
            countIWaitFor = NOWAIT_TAG;
            found = justRecieved;
            justRecieved = NULL;
            ASSERT_MALLOC_OK(found); // TODO: assert malloc ok nie powinno, ale
                                     // chce zobaczyc czy dziala
        }
        // Tutaj found ustawione na dobrą wiadomość, która odbieramy.

        printf("MAIN %d: Actually copying memory to target ptr\n", WORLD_RANK);
        memcpy(data, found->data, found->count);

        free(found);
        return MIMPI_SUCCESS;
    }
}

MIMPI_Retcode MIMPI_Barrier(){
    TODO
    // Przeprowadza synchronizacje wszystkich procesów? Czyli pewnie działa tak
    // jak zwykła barriera
}

MIMPI_Retcode MIMPI_Bcast(void *data, int count, int root){
    TODO
    // Wysyła dane zapewnione przez proces o randze root do WSZSYTKICH
    // pozostałych procesow
}

MIMPI_Retcode MIMPI_Reduce(void const *send_data, void *recv_data, int count,
                           MIMPI_Op op, int root) {
    TODO
}