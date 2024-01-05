/**
 * This file is for implementation of MIMPI library.
 * */

#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/select.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct sendData {
    int count;
    int tag;
    void *data;
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

    free(nodeToDelete->data);
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
        printf("%d -> ", temp->data);
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
/// LINKED LIST IMPLEMENTATION ///

/// GLOBALS ///
static bool DEADLOCK_DETECTION;

static int WORLD_SIZE;
static int WORLD_RANK;
int write_descriptors[MAX_PROCESSES + 1];
int read_descriptors[MAX_PROCESSES + 1];

// Helper thread data
bool helperThreadActive = true;
pthread_t helperThread;
fd_set readDescriptors;
int maxFd = 0;
pthread_mutex_t mutex;

LinkedList *buffer[MAX_PROCESSES];
/// GLOBALS END ///

void printMyDesc() {
    printf(
        "================================================================\n");
    printf("WRITE DESCRIPTORS\n");
    for (int i = 0; i < WORLD_SIZE; i++) {
        printf("%d | ", write_descriptors[i]);
    }
    printf("\n");
    printf("READ DESCRIPTORS\n");
    for (int i = 0; i < WORLD_SIZE; i++) {
        printf("%d | ", read_descriptors[i]);
    }
    printf("\n");

    printf(
        "================================================================\n");
}

void *helperThreadMain() {
    // Dodajemy wszystkie desktyptory do zbioru śledzonych
    FD_ZERO(&readDescriptors);
    for (int i = 0; i < WORLD_SIZE; i++) {
        FD_SET(read_descriptors[i], &readDescriptors);
    }

    while (helperThreadActive) {
        // Jeżeli nie ma żadnych gotowych desktyptorów to wątek czeka
        int ready = select(maxFd + 1, &readDescriptors, NULL, NULL, NULL);
        ASSERT_SYS_OK(ready);

        // TODO: Co jeżeli na jednym channelu czeka kilka wiadomości
        for (int i = 0; i < WORLD_SIZE; i++) {
            if (FD_ISSET(read_descriptors[i], &readDescriptors)) {
                // Wczytujemy tylko rozmiar danych, zeby wiedziec ile pamięci
                // zaalokować Ten while tutaj to troche z armaty do wróbla.
                // TODO: Może usuń while'a.
                int count;
                int totalRead = 0;
                int totalToBeRead = sizeof(int);
                while (totalRead < totalToBeRead) {
                    ssize_t read =
                        (read_descriptors[i], count + totalRead, sizeof(int));
                    ASSERT_SYS_OK(read);
                    totalRead += read;
                }

                sendData *recievedData =
                    (sendData *)malloc(2 * sizeof(int) + count);
                ASSERT_MALLOC_OK(recievedData);
                recievedData->count = count;

                // Czytamy tag i dane wielkosci count, count juz wczytany
                // TODO: mozliwe, ze tu trzeba zmniejszac totalToBeRead żeby nie
                // zaczać czytać kolejnej wiadomości
                totalRead = 0;
                totalToBeRead = sizeof(int) + recievedData->count;
                while (totalRead < totalToBeRead) {
                    ssize_t read = chread(
                        read_descriptors[i],
                        recievedData + sizeof(int) + totalRead, totalToBeRead);
                    ASSERT_SYS_OK(read);
                    totalRead += read;
                }

                // Tutaj recievedData powinno być już wypełnione. Można je dodać
                // do naszego bufora
                append(buffer[i], recievedData);

                // TODO: Obudzić proces, jeżeli czeka na ta wiadomość
            }
        }
    }
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

                write_descriptors[i] = writeDesc;
                read_descriptors[i] = readDesc;
                if (WORLD_RANK == 1)
                    printf("Ith: %d, %d\n", write_descriptors[i],
                           read_descriptors[i]);
            }
        }
    }

    // Inicjalizujemy bufor na senda.
    for (int i = 0; i < MAX_PROCESSES; i++) {
        buffer[i] = createLinkedList();
    }

    // Tworzymy wątek pomocniczy
    ASSERT_ZERO(pthread_mutex_init(&mutex, NULL));
    ASSERT_ZERO(pthread_create(&helperThread, NULL, helperThreadMain, NULL));
}

void MIMPI_Finalize() {

    // TODO: Musimy wysłać wiadomość do wszystkich procesów które czekały na
    // wiadomość od nas, ze wychodzimy z bloku

    // Ubijamy nasz helper thread
    helperThreadActive = false;
    ASSERT_ZERO(pthread_join(helperThread, NULL));
    ASSERT_ZERO(pthread_mutex_destroy(&mutex));

    // Niszczymy bufor na senda.
    for (int i = 0; i < MAX_PROCESSES; i++) {
        deleteLinkedList(buffer[i]);
    }

    // Zamykamy nie potrzebne deskrytptory
    for (int i = 0; i <= WORLD_SIZE; i++) {
        if (i != WORLD_RANK) {
            close(write_descriptors[i]);
            close(read_descriptors[i]);
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
    if (destination < 0 || destination > WORLD_SIZE - 1) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    } else if (destination == WORLD_RANK) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    } else {
        printf("Sending data to %d via %d\n", destination,
               write_descriptors[destination]);

        // TODO: Tu chyba malloc, chyba w sumie nie bo wysyłamy dane sendem
        // przed zniknieciem
        sendData *dataStruct;
        dataStruct->count = &count;
        dataStruct->tag = &tag;
        dataStruct->data = data;

        int structSize = count + 2 * sizeof(int);

        ssize_t total_sent = 0;
        while (total_sent < count) {
            ssize_t wrote = chsend(write_descriptors[destination],
                                   dataStruct + total_sent, structSize);
            ASSERT_SYS_OK(wrote);
            total_sent += wrote;
        }
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
    if (source < 0 || source > WORLD_SIZE - 1) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    } else if (source == WORLD_RANK) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    } else {
        



        printf("Recieving data from %d via %d\n", source,
               read_descriptors[source]);

        // TODO: Zmienić odbieranie, zeby wczytywac do structa
        ssize_t total_read = 0;
        while (total_read < count) {
            ssize_t read =
                chrecv(read_descriptors[source], data + total_read, count);
            ASSERT_SYS_OK(read);
        }
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