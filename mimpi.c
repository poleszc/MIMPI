#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"

#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/types.h>
#include <unistd.h>

#define CHUNK_SIZE 256

typedef struct initialDataChunk {
    int source;
    int count;
    int tag;
    void *data;
} initialDataChunk;

typedef struct dataChunk {
    int source;
    void *data;
} dataChunk;

/// DOUBLY LINKED LIST IMPLEMENTATION ///
// Struktura węzła listy
typedef struct Node {
    initialDataChunk *data;
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
    ASSERT_NOT_NULL(list);
    list->head = NULL;
    list->tail = NULL;
    return list;
}

// Funkcja dodająca węzeł na koniec listy
void append(LinkedList *list, initialDataChunk *data) {
    Node *newNode = (Node *)malloc(sizeof(Node));
    ASSERT_NOT_NULL(newNode);
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

    free(nodeToDelete);
}

// Funkcja dodająca węzeł po wskazanym węźle
void insertAfterNode(LinkedList *list, Node *prevNode, initialDataChunk *data) {
    if (prevNode == NULL) {
        printf("Previous node cannot be NULL\n");
        return;
    }

    Node *newNode = (Node *)malloc(sizeof(Node));
    ASSERT_NOT_NULL(newNode);
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
        fprintf(stderr, "(%d, %d) -> ", temp->data->tag, temp->data->count);
        temp = temp->next;
    }
    fprintf(stderr, "NULL\n");
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

/// DOUBLY LINKED LIST IMPLEMENTATION END ///

///////// MY GLOBALS /////////
// Oznacza, ze proces główny nie czeka na odebranie wiadomosci.
// Nie można go używać żeby wysyłać pomocnicze wiadomości do procesów.
#define NOWAIT_TAG -1
#define WORLD_SIZE_ENV "MIMPI_WORLD_SIZE"
#define WORLD_RANK_ENV "MIMPI_WORLD_RANK"
#define CHANNEL_READ_ENV "MIMPI_CHANNEL_READ"

bool DEADLOCK_DETECTION;
int WORLD_SIZE;
int WORLD_RANK;
// Did i-th program end.
bool PROGRAMS_ENDED[MAX_PROCESSES];
// FD for writing to i-th program.
int SEND_FDS[MAX_PROCESSES];
// FD for reading messages to ME.
int READ_FD;
// Messages being recieved from channel + their size/tag
initialDataChunk *MESSAGES_IN_BUILD[MAX_PROCESSES];
int BUILT_BY_NOW[MAX_PROCESSES];

bool remoteMainWaitedForEnded = false;

// Helper thread data
pthread_t helperThread;
// Buffer for messages from i-th program.
LinkedList *messageBuffer[MAX_PROCESSES];
sem_t mainThreadStopper; // Zatrzymuje watek glowny jak nie ma wiadomosci
pthread_mutex_t bufferMutex;
bool helperThreadActive = true;
// Na jaką wiadomość obecnie czeka wątek główny na mutexie.
int tagIWaitFor = NOWAIT_TAG;
int countIWaitFor = NOWAIT_TAG;
int sourceIWaitFor = NOWAIT_TAG;
bool recieved = true;
// Jeżeli helper odczytał dane, na które akurat czeka recv to nie wrzuca ich bez
// sensu do listy żeby uniknać podwójnego szukania
initialDataChunk *justRecieved = NULL;
///////// MY GLOBALS END /////////

void printBufferDebug(LinkedList *messageBuffer[MAX_PROCESSES]) {
    fprintf(stderr, "======================BUFFER======================\n");
    for (int i = 0; i < WORLD_SIZE; i++) {
        fprintf(stderr, "%d:   ", i);
        displayList(messageBuffer[i]);
    }
    fprintf(stderr, "==================================================\n");
}

initialDataChunk *searchAndRemoveMessage(LinkedList *list, int tag, int count) {
    fprintf(stderr, "Buffer in search\n");
    displayList(list);
    Node *curr = list->head;

    while (curr != list->tail && (tag == MIMPI_ANY_TAG && curr->data->count != count) &&
           (curr->data->tag != tag || curr->data->count != count)) {
        curr = curr->next;
    }

    if (curr == list->tail) { // Nie ma szukanej wiadomości
        return NULL;
    }

    initialDataChunk *data = curr->data;
    deleteNode(list, curr);
    return data;
}

// Arg t o indeks w tablicy wątków danego wątku.
void *helperThreadMain() {

    fprintf(stderr, "HELPER %d:  Entered main\n", WORLD_RANK);
    while (helperThreadActive) {
        void *recieved = (void *)malloc(CHUNK_SIZE);

        ssize_t read = chrecv(READ_FD, recieved, CHUNK_SIZE);
        ASSERT_SYS_OK(read);
        assert(read == 256);

        int source = *(int *)recieved;
        fprintf(stderr, "HELPER %d:  Read message from %d\n", WORLD_RANK, source);

        if (MESSAGES_IN_BUILD[source] == NULL) { // wiadomość incjalizujaca blok, wiemy ze jest tam count i
                                                 // tu alokujemy pamiec
            fprintf(stderr, "HELPER %d:  That was a first message\n", WORLD_RANK, source);
            initialDataChunk *data = (initialDataChunk *)recieved;

            initialDataChunk *bufferData = (initialDataChunk *)malloc(sizeof(initialDataChunk));
            bufferData->source = data->source;
            bufferData->tag = data->count;
            bufferData->count = data->tag;
            bufferData->data = (void *)malloc(bufferData->count);
            memcpy(bufferData->data, data->data, CHUNK_SIZE - 3 * sizeof(int));

            BUILT_BY_NOW[source] = CHUNK_SIZE - 3 * sizeof(int);
            MESSAGES_IN_BUILD[source] = bufferData;
        } else { // to juz kolejyny blok z kolei, dolaczamy do istniejacego
            fprintf(stderr, "HELPER %d:  Thats NOT a first message\n", WORLD_RANK, source);
            dataChunk *data = (dataChunk *)recieved;
            initialDataChunk *bufferData = MESSAGES_IN_BUILD[source];
            memcpy(bufferData->data + BUILT_BY_NOW[source], data->data, CHUNK_SIZE - sizeof(int));

            BUILT_BY_NOW[source] += (CHUNK_SIZE - sizeof(int));

            if (BUILT_BY_NOW[source] >= bufferData->count) { // Message ready
                if (bufferData->tag == -1) {                 // End request from parent
                    fprintf(stderr, "HELPER %d:  Got ending message from parent\n", WORLD_RANK);
                    break;
                } else if (bufferData->tag == -2) { // Process of id buff->source just ended
                    fprintf(stderr, "HELPER %d:  Process %d just ended\n", WORLD_RANK, bufferData->source);
                    PROGRAMS_ENDED[bufferData->source] = true;
                    // TODO: Tu chyba moze byc taka sytuacja, ze wysle wiadomosc,
                    // ale tez od razu info do nas ze sie skonczył.
                    // TODO: Chyba trzeba tu dodać sprawdzenie, ze pipe od niego
                    // jest na pewno pusty

                    // If someone waited for the process that just ended, we wake him up.
                    if (sourceIWaitFor == bufferData->source) {
                        fprintf(stderr, "HELPER %d: Main waited for message from %d\n", WORLD_RANK, bufferData->source);
                        remoteMainWaitedForEnded = true;
                        ASSERT_SYS_OK(sem_post(&mainThreadStopper));
                    }
                } else { // Finished creating a normal message

                    if (recieved && countIWaitFor == bufferData->count && sourceIWaitFor == bufferData->source &&
                        (tagIWaitFor == bufferData->tag || (tagIWaitFor == MIMPI_ANY_TAG && bufferData->tag >= 0))) {
                        fprintf(stderr, "HELPER %d: Someone was waiting for that exact message\n", WORLD_RANK);
                        recieved = false;
                        justRecieved = bufferData;
                        ASSERT_SYS_OK(sem_post(&mainThreadStopper));
                    } else {
                        fprintf(stderr, "HELPER %d: No one is waiting, appending to buffer\n", WORLD_RANK);
                        ASSERT_ZERO(pthread_mutex_lock(&bufferMutex));
                        append(messageBuffer[bufferData->source], bufferData->source);
                        printBufferDebug(messageBuffer);
                        ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
                    }
                }
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

    // Inicjalizujemy World Size i World Rank i deskryptory do komunikacji z
    // pomocnikiem
    const char *world_size_env = getenv(WORLD_SIZE_ENV);
    const char *world_rank_env = getenv(WORLD_RANK_ENV);
    const char *read_fd_env = getenv(CHANNEL_READ_ENV);
    ASSERT_NOT_NULL(world_size_env);
    ASSERT_NOT_NULL(world_rank_env);
    ASSERT_NOT_NULL(read_fd_env);

    WORLD_SIZE = atoi(world_size_env);
    WORLD_RANK = atoi(world_rank_env);
    READ_FD = atoi(read_fd_env);

    ASSERT_ZERO(unsetenv(WORLD_SIZE_ENV));
    ASSERT_ZERO(unsetenv(WORLD_RANK_ENV));
    ASSERT_ZERO(unsetenv(CHANNEL_READ_ENV));

    fprintf(stderr, "MAIN %d: Entered Init\n", WORLD_RANK);

    // Inicjalizujemy tablice deskryptorów do komunikacji send/recv z
    // programami.
    for (int i = 0; i < WORLD_SIZE; i++) {
        char writeDescEnv[32];
        snprintf(writeDescEnv, sizeof(writeDescEnv), "MIMPI_CHANNEL_WRITE_%d", i);

        const char *readFd = getenv(writeDescEnv);
        ASSERT_NOT_NULL(readFd);

        SEND_FDS[i] = atoi(readFd);
        unsetenv(writeDescEnv);
    }

    // Initializing global arrays and buffer.
    for (int i = 0; i < WORLD_SIZE; i++) {
        messageBuffer[i] = createLinkedList();
        PROGRAMS_ENDED[i] = false;
        MESSAGES_IN_BUILD[i] = NULL;
        // TODO: Sprawdzamy czy jest wiadomosc za pomoca nulla wiec moze to nie potrzebne
        BUILT_BY_NOW[i] = -1;
    }

    fprintf(stderr, "MAIN %d: Finished initializincg, startuing helper\n", WORLD_RANK);
    ASSERT_ZERO(pthread_mutex_init(&bufferMutex, NULL));
    ASSERT_SYS_OK(sem_init(&mainThreadStopper, 0, 0));
    ASSERT_ZERO(pthread_create(&helperThread, NULL, helperThreadMain, NULL));
}

void MIMPI_Finalize() {
    // TODO: Musimy wysłać wiadomość do wszystkich procesów które czekały na
    // wiadomość od nas, ze wychodzimy z bloku. Chyba właśnie nie wysyłamy,
    // tylko czekamy az read zwróci zero

    fprintf(stderr, "MAIN %d: Entered finalize\n", WORLD_RANK);

    // bool endMess = false;
    // for (int i = 0; i < WORLD_SIZE; i++)
    // {
    //     if (i != WORLD_RANK) {
    //         fprintf(stderr, "MAIN %d: Sending end message to %d\n",
    //         WORLD_RANK, i);

    //         int res = MIMPI_Send(&endMess, sizeof(endMess), i, -1);
    //         if (res != MIMPI_SUCCESS) {
    //             fprintf(stderr, "MAIN %d: MIMPI_SEND FAILED\n", WORLD_RANK);
    //         }
    //     }
    // }
    // int res = MIMPI_Send(&endMess, sizeof(endMess), WORLD_RANK, -1);
    // if (res != MIMPI_SUCCESS) {
    //     fprintf(stderr, "MAIN %d: MIMPI_SEND FAILED\n", WORLD_RANK);
    // }

    ASSERT_ZERO(pthread_join(helperThread, NULL));
    ASSERT_SYS_OK(sem_destroy(&mainThreadStopper));
    ASSERT_ZERO(pthread_mutex_destroy(&bufferMutex));

    // Niszczymy bufor na senda.
    for (int i = 0; i < WORLD_SIZE; i++) {
        deleteLinkedList(messageBuffer[i]);
    }

    // TODO: close my desctiptors

    channels_finalize();
    fprintf(stderr, "MAIN %d: Exiting finalize\n", WORLD_RANK);
}

int MIMPI_World_size() { return WORLD_SIZE; }

int MIMPI_World_rank() { return WORLD_RANK; }

MIMPI_Retcode MIMPI_Send(void const *data, int count, int destination, int tag) {

    // Jezeli wchodzimy tu i chcemy wysłać wiadomość do procesu do
    // destination ktory juz nie istnieje bo wyszedl z bloku MPI to zwracamy
    // MIMPI_ERROR_REMOTE_FINISHED Chyba tu nie zwracamy
    // MIMPI_DEADLOCK_DETECTED

    // TODO: Sprawdzenie czy program do ktorego wysyłamy wyszedł z bloku
    // MIMPI
    fprintf(stderr, "MAIN %d: Entering MIMPI_Send\n", WORLD_RANK);

    if (destination < 0 || destination > WORLD_SIZE - 1) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    } else if (destination == WORLD_RANK) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    } else if (PROGRAMS_ENDED[destination]) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    } else {
        fprintf(stderr,
                "MAIN %d: Initializing send to %d with count %d and tag %d, "
                "through %d",
                WORLD_RANK, destination, count, tag, SEND_FDS[destination]);

        // Pierwszy chunk z danymi i countem, kolejne będą miały tylko source
        int sentByNow = 0;

        initialDataChunk dataToSend;
        dataToSend.source = WORLD_RANK;
        dataToSend.count = count;
        dataToSend.tag = tag;
        dataToSend.data = (void *)malloc(CHUNK_SIZE - 3 * sizeof(int));
        memcpy(dataToSend.data, data, CHUNK_SIZE - 3 * sizeof(int));

        // TODO: Moze trzeba tu dodać while'a.
        ssize_t wrote = chsend(SEND_FDS[destination], &dataToSend, CHUNK_SIZE);
        ASSERT_SYS_OK(wrote);
        assert(wrote == 256);
        free(dataToSend.data);

        sentByNow += (CHUNK_SIZE - 3 * sizeof(int));

        fprintf(stderr, "MAIN %d: Sent initial chunk\n", WORLD_RANK);

        dataChunk chunk;
        chunk.source = WORLD_RANK;
        chunk.data = (void *)malloc(CHUNK_SIZE - sizeof(int));
        while (sentByNow < count) {
            fprintf(stderr, "MAIN %d: Chunk send\n", WORLD_RANK);
            memcpy(chunk.data, data + sentByNow, CHUNK_SIZE - sizeof(int));
            ssize_t wrote = chsend(SEND_FDS[destination], &chunk, CHUNK_SIZE);
            ASSERT_SYS_OK(wrote);
            // TODO : moze tu while
            assert(wrote == 256);
            sentByNow += (CHUNK_SIZE - sizeof(int));
        }
        free(chunk.data);

        fprintf(stderr, "MAIN %d: Sent all data\n", WORLD_RANK);
        return MIMPI_SUCCESS;
    }
}

MIMPI_Retcode MIMPI_Recv(void *data, int count, int source, int tag) {
    // Czeka na wiadomość o wielkosci count i znaczniku tag od procesu o
    // randze rank(zapisanej w source) Zapisuje ta wiadomość pod adresem
    // data, nie musimy martwic się ze cos tam nadpiszemy, bo to wołający ma
    // to ogarnac

    // TODO: Sprawdzenie czy program na ktory czekamy wyszedł z bloku MIMPI
    // i nie wysłał pasującej wiadomosci
    // TODO: Jeżeli proces tu wejdzie i czeka na wiadomość, a w między
    // czasie ten na ktory czekał wyjdzie to wychodzac musi wysłac ping do
    // procesow ktore czekały na wiadomosc od niego.
    remoteMainWaitedForEnded = false;
    fprintf(stderr, "MAIN %d: Entered MIMPI_Recv\n", WORLD_RANK);
    if (source < 0 || source > WORLD_SIZE - 1) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    } else if (source == WORLD_RANK) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    } else if (PROGRAMS_ENDED[source]) {
        fprintf(stderr, "MAIN %d: Source ended\n", WORLD_RANK);
        // TODO: Tu trzeba sprawdzić czy pipe od niego jest pusty, bo
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    // TODO: Tutaj można poddymić z tym readem zwracającym zero.
    // else if (wysylajacy się skończyl i nie wysłał pasujacej wiadomosci)
    // {}
    else {
        fprintf(stderr, "MAIN %d: Everything okay with the message, recieving\n", WORLD_RANK);

        ASSERT_ZERO(pthread_mutex_lock(&bufferMutex));
        initialDataChunk *found = searchAndRemoveMessage(messageBuffer[source], tag, count);
        ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));

        if (!found) { // Jeszcze nie ma wiadomości dla nas, czekamy.
            fprintf(stderr, "MAIN %d: No message was found, I hang myself\n", WORLD_RANK);

            // Source skończył się i nie wysłał wiadomości już przed naszym
            // czekaniem
            if (PROGRAMS_ENDED[source]) {
                fprintf(stderr, "MAIN %d: Remote ended\n", WORLD_RANK);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }

            recieved = true;
            tagIWaitFor = tag;
            countIWaitFor = count;
            sourceIWaitFor = source;
            ASSERT_SYS_OK(sem_wait(&mainThreadStopper));

            fprintf(stderr, "MAIN %d: Woke up from hanging\n", WORLD_RANK);

            tagIWaitFor = NOWAIT_TAG;
            countIWaitFor = NOWAIT_TAG;

            // Source zakończył się w trakcie naszego czekania na niego.
            if (remoteMainWaitedForEnded && recieved) {
                fprintf(stderr, "MAIN %d: Remote ended during our wating\n", WORLD_RANK);
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
            recieved = true;

            // Jak się tu obudziliśmy to na pewno jest dla nas wiadomość.
            // Oznaczmy ze nikt nie czeka, zeby pomocniczy wiedział i nie
            // musiał sprawdzac mutexa
            found = justRecieved;
            justRecieved = NULL;
            ASSERT_NOT_NULL(found);
        }
        // Tutaj found ustawione na dobrą wiadomość, która odbieramy.

        fprintf(stderr, "MAIN %d: Actually copying memory to target ptr\n", WORLD_RANK);
        memcpy(data, found->data, found->count);

        free(found);
        return MIMPI_SUCCESS;
    }
}

MIMPI_Retcode MIMPI_Barrier(){TODO}

MIMPI_Retcode MIMPI_Bcast(void *data, int count, int root){
    TODO
    // Wysyła dane zapewnione przez proces o randze root do WSZSYTKICH
    // pozostałych procesow
}

MIMPI_Retcode MIMPI_Reduce(void const *send_data, void *recv_data, int count, MIMPI_Op op, int root) {
    TODO
}