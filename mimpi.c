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

/// LINKED LIST IMPLEMENTATION ///












/// LINKED LIST IMPLEMENTATION ///
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

struct sendData {
    int count;
    int tag;
    void *data;
};

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
    // Dodajemy wszystkie desktyptory do zbioru tych, na które czekamy
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
                struct sendData *recievedData; // tu chyba trzeba mallockiem
                // Wczytujemy tylko rozmiar danych, zeby wiedziec ile trzeba
                // wczytac

                // Ten while tutaj to troche z armaty do wróbla. TODO: Może usuń
                // while'a.
                int totalRead = 0;
                int totalToBeRead = sizeof(int);
                while (totalRead < totalToBeRead) {
                    ssize_t read = (read_descriptors[i],
                                    recievedData + totalRead, sizeof(int));
                    ASSERT_SYS_OK(read);
                    totalRead += read;
                }

                // Czytamy tag i dane wielkosci count, count juz wczytany
                //TODO: mozliwe, ze tu trzeba zmniejszac totalToBeRead żeby nie zaczać czytać kolejnej wiadomości
                totalRead = 0;
                totalToBeRead = sizeof(int) + recievedData->count;
                while (totalRead < totalToBeRead) {
                    ssize_t read = chread(read_descriptors[i], recievedData + sizeof(int) + totalRead, totalToBeRead);
                    ASSERT_SYS_OK(read);
                    totalRead += read;
                }

                // Tutaj recievedData powinno być już wypełnione. Można je dodać do naszego bufora


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
    if (destination < 0 || destination > WORLD_SIZE) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    } else if (destination == WORLD_RANK) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    } else {
        printf("Sending data to %d via %d\n", destination,
               write_descriptors[destination]);

        struct sendData *dataStruct;
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
    if (source < 0 || source > WORLD_SIZE) {
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

    /*
    if (source didnt send any message and left the mpi block ||
        this process is waiting for a send but source leaves) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    else if () {
    }
    */
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