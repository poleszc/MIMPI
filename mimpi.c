#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"

#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct initialDataChunk {
    int source;
    int count;
    int tag;
    char *data;
} initialDataChunk;

void printMemory(void *ptr, size_t numBytes) {
    fprintf(stderr, "PRINT MEMORY\n");
    unsigned char *bytePtr = (unsigned char *)ptr;

    for (size_t i = 0; i < numBytes; i++) {
        if (i % 32 == 0) {
            fprintf(stderr, "\n");
        }
        fprintf(stderr, "%02hhX ", bytePtr[i]);
    }
    fprintf(stderr, "\n");
}

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
    nodeToDelete->data = NULL;
    free(nodeToDelete);
}

// Funkcja dodająca węzeł po wskazanym węźle
void insertAfterNode(LinkedList *list, Node *prevNode, initialDataChunk *data) {
    if (prevNode == NULL) {
        fprintf(stderr, "Previous node cannot be NULL\n");
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

// Finds and removes first message which matches the tag and count.
// Returns NULL if no such message was found.
initialDataChunk *searchAndRemoveMessage(LinkedList *list, int tag, int count) {
    Node *curr = list->head;

    if (!list->head && !list->tail) { // Buffer is empty.
        return NULL;
    }
    while (1) {
        if (!curr) { // curr is NULL, nothing was found
            return NULL;
        }
        if (tag == MIMPI_ANY_TAG && curr->data->count == count) {
            break;
        }
        if (curr->data->tag == tag && curr->data->count == count) {
            break;
        }
        curr = curr->next;
    }

    initialDataChunk *data = curr->data;
    deleteNode(list, curr);
    return data;
}

/// DOUBLY LINKED LIST IMPLEMENTATION END ///

///////// MY GLOBALS /////////
// Oznacza, ze proces główny nie czeka na odebranie wiadomosci.
// Nie można go używać żeby wysyłać pomocnicze wiadomości do procesów.
#define NOWAIT_TAG -1
#define REMOTE_FINISHED -2
#define MAIN_THREAD_FINISHED -3
#define BARRIER_TAG -4
#define BROADCAST_TAG -5
#define REDUCE_TAG -6
#define DEADLOCK_TAG -7

#define WORLD_SIZE_ENV "MIMPI_WORLD_SIZE"
#define WORLD_RANK_ENV "MIMPI_WORLD_RANK"
#define CHANNEL_READ_ENV "MIMPI_CHANNEL_READ"
#define CHUNK_SIZE 256

bool DEADLOCK_DETECTION_ENABLED;
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
int BYTES_READ_BY_NOW[MAX_PROCESSES];

bool remoteMainWaitedForEnded = false;

// Helper thread data
pthread_t helperThread;
// Buffer for messages from i-th program.
LinkedList *messageBuffer[MAX_PROCESSES];
sem_t mainThreadStopper; // Zatrzymuje watek glowny jak nie ma wiadomosci
pthread_mutex_t bufferMutex;
bool helperThreadActive = true;
// For what message is the main thread waiting.
int tagIWaitFor = NOWAIT_TAG;
int countIWaitFor = NOWAIT_TAG;
int sourceIWaitFor = NOWAIT_TAG;
// For direct passing to main thread if he waited for that exact message
initialDataChunk *justRecieved = NULL;
// Was the messege recieved by main if we passed it directly to him.
bool recieved = true;
// Buffers for MIMPI_Send and MIMPI_Recv
char helperChunk[CHUNK_SIZE];
char sendChunk[CHUNK_SIZE];

// Deadlock detection
// Did i-th process invoke MIMPI_Recv on me.
int recvOnMe[MAX_PROCESSES];
// To let the main thread now that deadlock was detected
bool deadlockDetected = false;
bool secondDeadlocked = false;

// Tree relations for logarythmic group functions
// [i][0] -> i-th parent
// [i][1] -> i-th left child
// [i][0] -> i-th right child
int treeRelations[MAX_PROCESSES][3] = {{-1, 1, 2},  {0, 3, 5},   {0, 4, 6},   {1, 7, 11},  {2, 9, 13},  {1, 8, 12},
                                       {2, 10, 14}, {3, 15, -1}, {5, -1, -1}, {4, -1, -1}, {6, -1, -1}, {3, -1, -1},
                                       {5, -1, -1}, {4, -1, -1}, {6, -1, -1}, {7, -1, -1}};

///////// MY GLOBALS END /////////

void printBufferDebug(LinkedList *messageBuffer[MAX_PROCESSES]) {
    fprintf(stderr, "======================BUFFER %d======================\n", WORLD_RANK);
    for (int i = 0; i < WORLD_SIZE; i++) {
        fprintf(stderr, "RANK %d , FROM %d:   ", WORLD_RANK, i);
        // displayList(messageBuffer[i]);
        Node *temp = messageBuffer[i]->head;
        while (temp != NULL) {
            fprintf(stderr, "(%d, %d) -> ", temp->data->tag, temp->data->count);
            temp = temp->next;
        }
        fprintf(stderr, "NULL\n");
    }
    fprintf(stderr, "==================================================\n");
}

int reduceTriple(uint8_t *left, uint8_t *right, const uint8_t *father, int count, MIMPI_Op op) {
    if (op == MIMPI_MAX) {
        for (int i = 0; i < count; i++) {
            if (right[i] > left[i] && right[i] > father[i]) {
                left[i] = right[i];
            } else if (father[i] > left[i] && father[i] > right[i]) {
                left[i] = father[i];
            }
        }
    } else if (op == MIMPI_MIN) {
        for (int i = 0; i < count; i++) {
            if (right[i] < left[i] && right[i] < father[i]) {
                left[i] = right[i];
            } else if (father[i] < left[i] && father[i] < right[i]) {
                left[i] = father[i];
            }
        }
    } else if (op == MIMPI_SUM) {
        for (int i = 0; i < count; i++) {
            left[i] += (right[i] + father[i]);
        }
    } else if (op == MIMPI_PROD) {
        for (int i = 0; i < count; i++) {
            left[i] *= (right[i] * father[i]);
        }
    } else { // Wrong operation
        return -1;
    }
    return 0;
}

int reduceDouble(uint8_t *left, const uint8_t *father, int count, MIMPI_Op op) {
    if (op == MIMPI_MAX) {
        for (int i = 0; i < count; i++) {
            left[i] = left[i] > father[i] ? left[i] : father[i];
        }
    } else if (op == MIMPI_MIN) {
        for (int i = 0; i < count; i++) {
            left[i] = left[i] < father[i] ? left[i] : father[i];
        }
    } else if (op == MIMPI_SUM) {
        for (int i = 0; i < count; i++) {
            left[i] += father[i];
        }
    } else if (op == MIMPI_PROD) {
        for (int i = 0; i < count; i++) {
            left[i] *= father[i];
        }
    } else { // Wrong operation
        return -1;
    }
    return 0;
}

void *helperThreadMain() {
    // fprintf(stderr, "HELPER %d:  Entered main\n", WORLD_RANK);
    while (helperThreadActive) {
        // fprintf(stderr, "HELPER %d:  Waiting on chrecv\n", WORLD_RANK);

        // We always recieve 256 byte blocks, although they are not always filled with data
        ssize_t read = chrecv(READ_FD, helperChunk, CHUNK_SIZE);
        ASSERT_SYS_OK(read);
        assert(read == 256);
        // fprintf(stderr, "HELPER %d:  Woke up from chrecv. Read %ld bytes\n", WORLD_RANK, read);

        if (DEADLOCK_DETECTION_ENABLED) {
            ASSERT_ZERO(pthread_mutex_lock(&bufferMutex));
        }

        int source;
        memcpy(&source, helperChunk, sizeof(int));
        // fprintf(stderr, "HELPER %d:  Read message from %d\n", WORLD_RANK, source);

        initialDataChunk *bufferData = NULL;
        if (MESSAGES_IN_BUILD[source] == NULL) { // Block initializing message. New message from source.
            int count = *(int *)(helperChunk + sizeof(int));
            int tag = *(int *)(helperChunk + 2 * sizeof(int));

            if (tag == REMOTE_FINISHED) { // Process of id source just ended.
                if (DEADLOCK_DETECTION_ENABLED) {
                    ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
                }
                // fprintf(stderr, "HELPER %d:  Process %d just ended\n", WORLD_RANK, source);
                PROGRAMS_ENDED[source] = true;
                // If main was waiting for the process that just ended, we wake him up and tell him to crash.
                if (sourceIWaitFor == source) {
                    // fprintf(stderr, "HELPER %d: Main waited for message from %d\n", WORLD_RANK, source);
                    remoteMainWaitedForEnded = true;
                    ASSERT_SYS_OK(sem_post(&mainThreadStopper));
                }
                continue;
            } else if (tag == MAIN_THREAD_FINISHED) { // End request from parent.
                // fprintf(stderr, "HELPER %d:  Got ending message from parent\n", WORLD_RANK);
                if (DEADLOCK_DETECTION_ENABLED) {
                    ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
                }
                break;
            } else if (DEADLOCK_DETECTION_ENABLED && tag == DEADLOCK_TAG) {
                if (count == sizeof(char)) { // Someone invoked recv on us.
                    recvOnMe[source]++;
                } else if (count == sizeof(int)) { // Deadlock occured.
                    // fprintf(stderr, "HELPER %d:  Second Deadlocked\n", WORLD_RANK);
                    secondDeadlocked = true;
                    deadlockDetected = true;
                    sourceIWaitFor = NOWAIT_TAG;
                    countIWaitFor = NOWAIT_TAG;
                    tagIWaitFor = NOWAIT_TAG;
                    // fprintf(stderr, "HELPER %d:  Waking up main\n", WORLD_RANK);
                    ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
                    ASSERT_SYS_OK(sem_post(&mainThreadStopper));
                    continue;
                }

                // this process is already waiting for the one that wants to wait for him/
                if (recvOnMe[source] > 0 && sourceIWaitFor == source) {
                    // We wake up the main thread and tell him to crash.
                    // fprintf(stderr, "HELPER %d:  Waking up my main cause DEADLOCK occured\n", WORLD_RANK);
                    deadlockDetected = true;
                    sourceIWaitFor = NOWAIT_TAG;
                    countIWaitFor = NOWAIT_TAG;
                    tagIWaitFor = NOWAIT_TAG;
                    ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
                    ASSERT_SYS_OK(sem_post(&mainThreadStopper));
                } else {
                    ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
                }
                // Deadlock wasn't detected, we mark the source as deadlockable
                continue;
            } else {
                if (DEADLOCK_DETECTION_ENABLED) {
                    ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
                }
            }

            bufferData = (initialDataChunk *)malloc(sizeof(initialDataChunk));
            bufferData->source = source;
            bufferData->count = count;
            bufferData->tag = tag;
            bufferData->data = malloc(bufferData->count);
            ASSERT_NOT_NULL(bufferData->data);

            int copySize = count < CHUNK_SIZE - 3 * sizeof(int) ? count : CHUNK_SIZE - 3 * sizeof(int);
            memcpy(bufferData->data, helperChunk + 3 * sizeof(int), copySize);

            BYTES_READ_BY_NOW[source] = copySize;
            MESSAGES_IN_BUILD[source] = bufferData;
        } else { // Not the first block, appending to message in build.
            if (DEADLOCK_DETECTION_ENABLED) {
                ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
            }
            bufferData = MESSAGES_IN_BUILD[source];
            int copySize = bufferData->count - BYTES_READ_BY_NOW[bufferData->source] < CHUNK_SIZE - sizeof(int)
                               ? bufferData->count - BYTES_READ_BY_NOW[bufferData->source]
                               : CHUNK_SIZE - sizeof(int);

            memcpy(bufferData->data + BYTES_READ_BY_NOW[source], helperChunk + sizeof(int), copySize);
            BYTES_READ_BY_NOW[source] += copySize;
        }

        // We built the whole message, now we have to send it to main.
        if (BYTES_READ_BY_NOW[source] == MESSAGES_IN_BUILD[source]->count) {
            // Main was waiting for that exact message
            if (recieved && countIWaitFor == bufferData->count && sourceIWaitFor == bufferData->source &&
                (tagIWaitFor == bufferData->tag || (tagIWaitFor == MIMPI_ANY_TAG && bufferData->tag >= 0))) {
                // fprintf(stderr, "HELPER %d: Someone was waiting for that exact message\n", WORLD_RANK);
                recieved = false;
                justRecieved = bufferData;
                MESSAGES_IN_BUILD[source] = NULL;
                BYTES_READ_BY_NOW[source] = -1;
                sourceIWaitFor = NOWAIT_TAG;
                ASSERT_SYS_OK(sem_post(&mainThreadStopper));
            } else { // Main didn't wait for that message, adding to buffer
                // fprintf(stderr, "HELPER %d: No one is waiting, appending to buffer\n", WORLD_RANK);
                append(messageBuffer[source], bufferData);
                MESSAGES_IN_BUILD[source] = NULL;
                BYTES_READ_BY_NOW[source] = -1;
            }
        }
    }
    // fprintf(stderr, "HELPER %d: I die :(\n", WORLD_RANK);
    return NULL;
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    DEADLOCK_DETECTION_ENABLED = enable_deadlock_detection;

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

    // fprintf(stderr, "MAIN %d: Entered Init\n", WORLD_RANK);

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
        BYTES_READ_BY_NOW[i] = -1;
        recvOnMe[i] = 0;
    }

    // fprintf(stderr, "MAIN %d: Finished initializing, starting helper\n", WORLD_RANK);
    ASSERT_ZERO(pthread_mutex_init(&bufferMutex, NULL));
    ASSERT_SYS_OK(sem_init(&mainThreadStopper, 0, 0));
    ASSERT_ZERO(pthread_create(&helperThread, NULL, helperThreadMain, NULL));
}

void MIMPI_Finalize() {
    // fprintf(stderr, "MAIN %d: Entered finalize\n", WORLD_RANK);
    int endMess = __INT_MAX__;
    for (int i = 0; i < WORLD_SIZE; i++) {
        if (i != WORLD_RANK && !PROGRAMS_ENDED[i]) {
            int res = MIMPI_Send(&endMess, sizeof(endMess), i, REMOTE_FINISHED);
            if (res != MIMPI_SUCCESS) {
                fprintf(stderr, "MAIN %d: MIMPI_SEND FAILED: %d\n", WORLD_RANK, res);
            }
        }
    }
    // Sending message to ourself to stop helper.
    int res = MIMPI_Send(&endMess, sizeof(endMess), WORLD_RANK, MAIN_THREAD_FINISHED);
    if (res != MIMPI_SUCCESS) {
        fprintf(stderr, "MAIN %d: MIMPI_SEND FAILED\n", WORLD_RANK);
    }

    ASSERT_ZERO(pthread_join(helperThread, NULL));
    ASSERT_SYS_OK(sem_destroy(&mainThreadStopper));
    ASSERT_ZERO(pthread_mutex_destroy(&bufferMutex));

    // Niszczymy bufor na senda.
    for (int i = 0; i < WORLD_SIZE; i++) {
        deleteLinkedList(messageBuffer[i]);
    }

    // TODO: close my desctiptors

    channels_finalize();
    // fprintf(stderr, "MAIN %d: Exiting finalize\n", WORLD_RANK);
}

int MIMPI_World_size() { return WORLD_SIZE; }

int MIMPI_World_rank() { return WORLD_RANK; }

MIMPI_Retcode MIMPI_Send(void const *data, int count, int destination, int tag) {
    // fprintf(stderr, "MAIN %d: Entering MIMPI_Send D %d, C %d, T %d\n", WORLD_RANK, destination, count, tag);
    if (destination < 0 || destination > WORLD_SIZE - 1) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    } else if (destination == WORLD_RANK &&
               tag != MAIN_THREAD_FINISHED) { // tag -2 is an ending message to helper so we allow self send
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    } else if (PROGRAMS_ENDED[destination]) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    } else {
        // fprintf(stderr, "MAIN %d: Initializing send to %d with count %d and tag %d, through %d\n", WORLD_RANK,
        // destination, count, tag, SEND_FDS[destination]);

        // Pierwszy chunk z danymi i countem, kolejne będą miały tylko source
        // In first data chunk, first 3*sizeof(int) bytes are used to store source, count and tag. Rest is for the data.
        memcpy(sendChunk, &WORLD_RANK, sizeof(int));
        memcpy(sendChunk + sizeof(int), &count, sizeof(int));
        memcpy(sendChunk + 2 * sizeof(int), &tag, sizeof(int));
        memcpy(sendChunk + 3 * sizeof(int), data, CHUNK_SIZE - 3 * sizeof(int));

        int sentByNow = 0;

        ssize_t wrote = chsend(SEND_FDS[destination], &sendChunk, CHUNK_SIZE);
        ASSERT_SYS_OK(wrote);
        assert(wrote == 256);
        sentByNow += (CHUNK_SIZE - 3 * sizeof(int));

        // fprintf(stderr, "MAIN %d: Sent initial chunk\n", WORLD_RANK);

        int counter = 0;
        while (sentByNow < count) {
            int toCopy = CHUNK_SIZE - sizeof(int) < count - sentByNow ? CHUNK_SIZE - sizeof(int) : count - sentByNow;
            memcpy(sendChunk + sizeof(int), data + sentByNow, toCopy);
            ssize_t wrote = chsend(SEND_FDS[destination], &sendChunk, CHUNK_SIZE);
            ASSERT_SYS_OK(wrote);
            assert(wrote == 256);
            sentByNow += toCopy;
            counter++;
        }

        // fprintf(stderr, "MAIN %d: Sent all data\n", WORLD_RANK);
        return MIMPI_SUCCESS;
    }
}

MIMPI_Retcode MIMPI_Recv(void *data, int count, int source, int tag) {
    // fprintf(stderr, "MAIN %d: Entered MIMPI_Recv: S %d, C %d, T %d\n", WORLD_RANK, source, count, tag);
    remoteMainWaitedForEnded = false;
    if (source < 0 || source > WORLD_SIZE - 1) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    } else if (source == WORLD_RANK) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }

    ASSERT_ZERO(pthread_mutex_lock(&bufferMutex));
    initialDataChunk *found = searchAndRemoveMessage(messageBuffer[source], tag, count);
    if (DEADLOCK_DETECTION_ENABLED) {
        // fprintf(stderr, "MAIN %d: Deadlock block\n", WORLD_RANK);
        if (WORLD_RANK < source) { // If we have a smaller index, then we ask for permission
            // fprintf(stderr, "MAIN %d: Smaller index sending message to source\n", WORLD_RANK);
            char deadlockMessage = 1;
            // We send source a message to let him now that we invoked recv on him.
            int res = MIMPI_Send(&deadlockMessage, sizeof(char), source, DEADLOCK_TAG);
            if (res == MIMPI_ERROR_REMOTE_FINISHED) {
                ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
        } else if (!found) {
            // fprintf(stderr, "MAIN %d: Bigger index than source. Checking if he came\n", WORLD_RANK);
            if (recvOnMe[source] > 0) { // This process invoked recv on me. I need to crash, and let him now
                recvOnMe[source]--;
                // fprintf(stderr, "MAIN %d: source %d invoked recv on me\n", WORLD_RANK, source);
                ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
                int deadlockMessage = 8054;
                int res = MIMPI_Send(&deadlockMessage, sizeof(int), source, DEADLOCK_TAG);
                if (res == MIMPI_ERROR_REMOTE_FINISHED) {
                    return MIMPI_ERROR_REMOTE_FINISHED;
                }
                return MIMPI_ERROR_DEADLOCK_DETECTED;
            }
        }
    }
    // fprintf(stderr, "MAIN %d: Got through deadlock block\n", WORLD_RANK);

    if (!found && PROGRAMS_ENDED[source]) {
        // fprintf(stderr, "MAIN %d: Source ended\n", WORLD_RANK);
        ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
        return MIMPI_ERROR_REMOTE_FINISHED;
    } else {
        if (!found) { // Jeszcze nie ma wiadomości dla nas, czekamy.
            // fprintf(stderr, "MAIN %d: No message was found\n", WORLD_RANK);

            // Source skończył się i nie wysłał wiadomości już przed naszym
            // czekaniem
            if (PROGRAMS_ENDED[source]) {
                // fprintf(stderr, "MAIN %d: Remote ended\n", WORLD_RANK);
                ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
                return MIMPI_ERROR_REMOTE_FINISHED;
            }

            recieved = true;
            tagIWaitFor = tag;
            countIWaitFor = count;
            sourceIWaitFor = source;
            ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
            // fprintf(stderr, "MAIN %d: I hang myself, waiting for: S %d, C %d, T %d\n", WORLD_RANK, source, count,
            // tag);
            ASSERT_SYS_OK(sem_wait(&mainThreadStopper));

            // fprintf(stderr, "MAIN %d: Woke up from hanging\n", WORLD_RANK);

            sourceIWaitFor = NOWAIT_TAG;
            tagIWaitFor = NOWAIT_TAG;
            countIWaitFor = NOWAIT_TAG;

            if (recieved && DEADLOCK_DETECTION_ENABLED &&
                deadlockDetected) { // We woke up because a deadlock was detected
                // We let the other procces know if it has a smaller index.
                // fprintf(stderr, "MAIN %d:  Woke up beacuse of deadlock\n", WORLD_RANK);
                deadlockDetected = false;
                recvOnMe[source]--; // TODO: That may be a farmazon.
                // printRecvOnMe();
                // if (source <  WORLD_RANK) { // If we were bigger of the two
                if (!secondDeadlocked) { // So we wake up the partner only once
                    int tempMessage = -2;
                    // fprintf(stderr, "MAIN %d:  Let now source %d that deadlock occured\n", WORLD_RANK, source);
                    int res = MIMPI_Send(&tempMessage, sizeof(int), source, DEADLOCK_TAG);
                    ASSERT_ZERO(res);
                } else {
                    // fprintf(stderr, "MAIN %d:  Im the second deadlocked\n", WORLD_RANK);
                    secondDeadlocked = false;
                }

                // fprintf(stderr, "MAIN %d:  DEADLOCK CRASH\n", WORLD_RANK);
                return MIMPI_ERROR_DEADLOCK_DETECTED;
            } else if (remoteMainWaitedForEnded &&
                       recieved) { // Source zakończył się w trakcie naszego czekania na niego.
                // fprintf(stderr, "MAIN %d: Remote ended during our wating\n", WORLD_RANK);
                if (DEADLOCK_DETECTION_ENABLED) {
                    recvOnMe[source]--; // TODO: ptentially buggable
                    // printRecvOnMe();
                }
                return MIMPI_ERROR_REMOTE_FINISHED;
            }

            // if (source < WORLD_RANK && DEADLOCK_DETECTION_ENABLED) {
            //     recvOnMe[source]--;
            //     printRecvOnMe();
            // }
            recieved = true; // cos z tym recieved moze byc nie tak

            // Jak się tu obudziliśmy to na pewno jest dla nas wiadomość.
            // Oznaczmy ze nikt nie czeka, zeby pomocniczy wiedział i nie
            // musiał sprawdzac mutexa
            found = justRecieved;
            justRecieved = NULL;
            ASSERT_NOT_NULL(found);
        } else {
            ASSERT_ZERO(pthread_mutex_unlock(&bufferMutex));
        }
        // Tutaj found ustawione na dobrą wiadomość, która odbieramy.
        if (DEADLOCK_DETECTION_ENABLED && source < WORLD_RANK) {
            recvOnMe[source]--;
        }

        // fprintf(stderr, "MAIN %d: Actually copying memory to target ptr\n", WORLD_RANK);
        memcpy(data, found->data, found->count);

        free(found);
        // fprintf(stderr, "MAIN %d: Leaving recv\n", WORLD_RANK);
        return MIMPI_SUCCESS;
    }
}

MIMPI_Retcode MIMPI_Barrier() {
    // fprintf(stderr, "MAIN %d: Enterend MIMPI_Barrier\n", WORLD_RANK);
    int barrierMessage = 420;
    int res;
    if (treeRelations[WORLD_RANK][1] < WORLD_SIZE && treeRelations[WORLD_RANK][1] != -1) {
        // fprintf(stderr, "MAIN %d: Waiting for message from left son\n", WORLD_RANK);
        res = MIMPI_Recv(&barrierMessage, sizeof(int), treeRelations[WORLD_RANK][1], BARRIER_TAG);
        // fprintf(stderr, "MAIN %d: Got message from left son\n", WORLD_RANK);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    if (treeRelations[WORLD_RANK][2] < WORLD_SIZE && treeRelations[WORLD_RANK][2] != -1) {
        // fprintf(stderr, "MAIN %d: Waiting for message from right son\n", WORLD_RANK);
        res = MIMPI_Recv(&barrierMessage, sizeof(int), treeRelations[WORLD_RANK][2], BARRIER_TAG);
        // fprintf(stderr, "MAIN %d: Got message from right son\n", WORLD_RANK);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    if (WORLD_RANK != 0) { // We send the message to our parent, then we wait for to parent to respond. After that
                           // we wake up out children
        barrierMessage = 420;
        // fprintf(stderr, "MAIN %d: Sending message to our parent\n", WORLD_RANK);
        res = MIMPI_Send(&barrierMessage, sizeof(int), treeRelations[WORLD_RANK][0], BARRIER_TAG);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        // fprintf(stderr, "MAIN %d: Waiting for message from our parent\n", WORLD_RANK);
        res = MIMPI_Recv(&barrierMessage, sizeof(int), treeRelations[WORLD_RANK][0], BARRIER_TAG);
        // fprintf(stderr, "MAIN %d: Got message from our parent\n", WORLD_RANK);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        if (treeRelations[WORLD_RANK][1] < WORLD_SIZE && treeRelations[WORLD_RANK][1] != -1) {
            // fprintf(stderr, "MAIN %d: Waking up left son\n", WORLD_RANK);
            res = MIMPI_Send(&barrierMessage, sizeof(int), treeRelations[WORLD_RANK][1], BARRIER_TAG);
            if (res == MIMPI_ERROR_REMOTE_FINISHED) {
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
        }
        if (treeRelations[WORLD_RANK][2] < WORLD_SIZE && treeRelations[WORLD_RANK][2] != -1) {
            // fprintf(stderr, "MAIN %d: Waking up right son\n", WORLD_RANK);
            res = MIMPI_Send(&barrierMessage, sizeof(int), treeRelations[WORLD_RANK][2], BARRIER_TAG);
            if (res == MIMPI_ERROR_REMOTE_FINISHED) {
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
        }
    } else { // if zero gets a message it starts waking everyone up
        // fprintf(stderr, "MAIN %d: ROOT GOT PINGED\n", WORLD_RANK);
        if (treeRelations[WORLD_RANK][1] < WORLD_SIZE && treeRelations[WORLD_RANK][1] != -1) {
            // fprintf(stderr, "MAIN %d: ROOT wakes up first child\n", WORLD_RANK);
            res = MIMPI_Send(&barrierMessage, sizeof(int), treeRelations[WORLD_RANK][1], BARRIER_TAG);
            if (res == MIMPI_ERROR_REMOTE_FINISHED) {
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
        }
        if (treeRelations[WORLD_RANK][2] < WORLD_SIZE && treeRelations[WORLD_RANK][2] != -1) {
            // fprintf(stderr, "MAIN %d: ROOT wakes up second child\n", WORLD_RANK);
            res = MIMPI_Send(&barrierMessage, sizeof(int), treeRelations[WORLD_RANK][2], BARRIER_TAG);
            if (res == MIMPI_ERROR_REMOTE_FINISHED) {
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
        }
    }
    // fprintf(stderr, "MAIN %d: Walks out of MIMPI_Barrier\n", WORLD_RANK);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(void *data, int count, int root) {
    // fprintf(stderr, "MAIN %d: Entered MIMPI_Bcast C %d, R %d\n", WORLD_RANK, count, root);
    if (root < 0 || root > WORLD_SIZE - 1) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    int barrierMessage = 420;
    int res;
    bool rootChange = (root != 0);

    int myIx = WORLD_RANK;
    if (WORLD_RANK == root) { // Substituting root with zero
        myIx = 0;
    } else if (WORLD_RANK == 0 && root != 0) {
        myIx = root;
    }

    int leftSon = treeRelations[myIx][1];
    int rightSon = treeRelations[myIx][2];
    if (WORLD_RANK == root && treeRelations[myIx][1] == WORLD_RANK) {
        leftSon = 0;
    } else if (treeRelations[myIx][1] == root) {
        leftSon = 0;
    }
    if (WORLD_RANK == root && treeRelations[myIx][2] == WORLD_RANK) {
        rightSon = 0;
    } else if (treeRelations[myIx][2] == root) {
        rightSon = 0;
    }

    // fprintf(stderr, "MAIN %d: Got index %d\n", WORLD_RANK, myIx);
    if (leftSon < WORLD_SIZE && leftSon != -1) {
        // fprintf(stderr, "MAIN %d INDEX %d: Waiting for message from left son %d\n", WORLD_RANK, myIx, leftSon);
        res = MIMPI_Recv(&barrierMessage, sizeof(int), leftSon, BROADCAST_TAG);
        // fprintf(stderr, "MAIN %d INDEX %d: Got message from left son\n", WORLD_RANK, myIx);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    if (rightSon < WORLD_SIZE && rightSon != -1) {
        // fprintf(stderr, "MAIN %d INDEX %d: Waiting for message from right son %d\n", WORLD_RANK, myIx, rightSon);
        res = MIMPI_Recv(&barrierMessage, sizeof(int), rightSon, BROADCAST_TAG);
        // fprintf(stderr, "MAIN %d INDEX %d: Got message from right son\n", WORLD_RANK, myIx);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    if (WORLD_RANK == root) {
        // Start sending the message down the tree.
        // fprintf(stderr, "MAIN %d INDEX %d: ROOT STARTS HIS WAY DOWN\n", WORLD_RANK, myIx);
        if (leftSon < WORLD_SIZE && leftSon != -1) {
            // fprintf(stderr, "MAIN %d INDEX %d: ROOT wakes up left child %d\n", WORLD_RANK, myIx, leftSon);
            res = MIMPI_Send(data, count, leftSon, BROADCAST_TAG);
            if (res == MIMPI_ERROR_REMOTE_FINISHED) {
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
        }
        if (rightSon < WORLD_SIZE && rightSon != -1) {
            // fprintf(stderr, "MAIN %d INDEX %d: ROOT wakes up right child %d\n", WORLD_RANK, myIx, rightSon);
            res = MIMPI_Send(data, count, rightSon, BROADCAST_TAG);
            if (res == MIMPI_ERROR_REMOTE_FINISHED) {
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
        }
    } else {
        barrierMessage = 420;

        // We're sending the message to roots parent. It could be us, if so, we know we need to send it directly to
        // root
        int parent = treeRelations[myIx][0];
        if (WORLD_RANK == 0 && treeRelations[myIx][0] == 0) {
            parent = root;
        } else if (rootChange && treeRelations[myIx][0] == 0) {
            parent = root;
        } else if (rootChange && treeRelations[myIx][0] == root) {
            parent = 0;
        }

        // fprintf(stderr, "MAIN %d INDEX %d: Sending message to our parent %d\n", WORLD_RANK, myIx, parent);

        res = MIMPI_Send(&barrierMessage, sizeof(int), parent, BROADCAST_TAG);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        // Waiting for the message from above. It should contain the data
        // fprintf(stderr, "MAIN %d INDEX %d: Waiting for message from our parent %d\n", WORLD_RANK, myIx, parent);
        res = MIMPI_Recv(data, count, parent, BROADCAST_TAG);
        // fprintf(stderr, "MAIN %d INDEX %d: Got message from our parent\n", WORLD_RANK, myIx);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        if (leftSon < WORLD_SIZE && leftSon != -1) {
            // fprintf(stderr, "MAIN %d INDEX %d: Waking up left son %d\n", WORLD_RANK, myIx, leftSon);
            res = MIMPI_Send(data, count, leftSon, BROADCAST_TAG);
            if (res == MIMPI_ERROR_REMOTE_FINISHED) {
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
        }
        if (rightSon < WORLD_SIZE && rightSon != -1) {
            // fprintf(stderr, "MAIN %d INDEX %d: Waking up right son %d\n", WORLD_RANK, myIx, rightSon);
            res = MIMPI_Send(data, count, rightSon, BROADCAST_TAG);
            if (res == MIMPI_ERROR_REMOTE_FINISHED) {
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
        }
    }

    // fprintf(stderr, "MAIN %d INDEX %d: Leaving Bcast\n", WORLD_RANK, myIx);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Reduce(void const *send_data, void *recv_data, int count, MIMPI_Op op, int root) {
    // fprintf(stderr, "MAIN %d: Entered MIMPI_Reduce C%d R %d O %d\n", WORLD_RANK, count, root, op);
    if (root < 0 || root > WORLD_SIZE - 1) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    void *leftSonMess = malloc(count);
    void *rightSonMess = malloc(count);
    int reduceMessage = 420;
    int res;
    bool rootChange = (root != 0);

    int myIx = WORLD_RANK;
    if (WORLD_RANK == root) { // Substituting root with zero
        myIx = 0;
    } else if (WORLD_RANK == 0 && root != 0) {
        myIx = root;
    }

    int leftSon = treeRelations[myIx][1];
    int rightSon = treeRelations[myIx][2];
    bool leftExists = false;
    bool rightExists = false;
    if (WORLD_RANK == root && treeRelations[myIx][1] == WORLD_RANK) {
        leftSon = 0;
    } else if (treeRelations[myIx][1] == root) {
        leftSon = 0;
    }
    if (WORLD_RANK == root && treeRelations[myIx][2] == WORLD_RANK) {
        rightSon = 0;
    } else if (treeRelations[myIx][2] == root) {
        rightSon = 0;
    }

    // fprintf(stderr, "MAIN %d: Got index %d\n", WORLD_RANK, myIx);
    if (leftSon < WORLD_SIZE && leftSon != -1) {
        // fprintf(stderr, "MAIN %d INDEX %d: Waiting for message from left son %d\n", WORLD_RANK, myIx, leftSon);
        leftExists = true;
        res = MIMPI_Recv(leftSonMess, count, leftSon, REDUCE_TAG);
        // fprintf(stderr, "MAIN %d INDEX %d: Got message from left son\n", WORLD_RANK, myIx);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    if (rightSon < WORLD_SIZE && rightSon != -1) {
        // fprintf(stderr, "MAIN %d INDEX %d: Waiting for message from right son %d\n", WORLD_RANK, myIx, rightSon);
        rightExists = true;
        res = MIMPI_Recv(rightSonMess, count, rightSon, REDUCE_TAG);
        // fprintf(stderr, "MAIN %d INDEX %d: Got message from right son\n", WORLD_RANK, myIx);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    if (leftExists && rightExists) {
        // fprintf(stderr, "MAIN %d INDEX %d: Triple reduce\n", WORLD_RANK, myIx);
        int reduceRes = reduceTriple(leftSonMess, rightSonMess, send_data, count, op);
        assert(reduceRes == 0);
    } else if (leftExists) {
        // fprintf(stderr, "MAIN %d INDEX %d: Double reduce\n", WORLD_RANK, myIx);
        int reduceRes = reduceDouble(leftSonMess, send_data, count, op);
        assert(reduceRes == 0);
    }
    free(rightSonMess);

    // After that the reduced message is in leftSonMess
    if (WORLD_RANK == root) {
        // fprintf(stderr, "MAIN %d INDEX %d: ROOT copies the data to recv_data\n", WORLD_RANK, myIx);
        memcpy(recv_data, leftSonMess, count);

    } else {
        int parent = treeRelations[myIx][0];
        if (WORLD_RANK == 0 && treeRelations[myIx][0] == 0) {
            parent = root;
        } else if (rootChange && treeRelations[myIx][0] == 0) {
            parent = root;
        } else if (rootChange && treeRelations[myIx][0] == root) {
            parent = 0;
        }

        // Sending reduced message to out parents
        if (!leftExists && !rightExists) {
            // fprintf(stderr, "MAIN %d INDEX %d: IM A LEAF Sending message to parent %d\n", WORLD_RANK, myIx,
            // parent);
            res = MIMPI_Send(send_data, count, parent, REDUCE_TAG);
        } else {
            // fprintf(stderr, "MAIN %d INDEX %d: Sending message to parent %d\n", WORLD_RANK, myIx, parent);
            res = MIMPI_Send(leftSonMess, count, parent, REDUCE_TAG);
        }
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        // Waiting for message from our parent
        res = MIMPI_Recv(&reduceMessage, sizeof(int), parent, REDUCE_TAG);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    // We're ready to leave. Sending message to sons.
    if (leftExists) {
        // fprintf(stderr, "MAIN %d INDEX %d: Sending message to our left son %d\n", WORLD_RANK, myIx, leftSon);
        res = MIMPI_Send(&reduceMessage, sizeof(int), leftSon, REDUCE_TAG);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    if (rightExists) {
        // fprintf(stderr, "MAIN %d INDEX %d: Sending message to our right son %d\n", WORLD_RANK, myIx, rightSon);
        rightExists = true;
        res = MIMPI_Send(&reduceMessage, sizeof(int), rightSon, REDUCE_TAG);
        if (res == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }
    // fprintf(stderr, "MAIN %d INDEX %d: Leaving reduce\n", WORLD_RANK, myIx);

    return MIMPI_SUCCESS;
}