/**
 * This file is for implementation of MIMPI library.
 * */

#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"

#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>

static bool DEADLOCK_DETECTION;
static int WORLD_SIZE;
static int WORLD_RANK;
int write_descriptors[MAX_PROCESSES + 1];
int read_descriptors[MAX_PROCESSES + 1];

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    DEADLOCK_DETECTION = enable_deadlock_detection;
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
    for (int i = 1; i <= MAX_PROCESSES; i++) {
        if (i != WORLD_RANK) {
            char write_descriptor[32];
            snprintf(write_descriptor, sizeof(write_descriptor),
                    "MIMPI_CHANNEL_WRITE_%d_%d", WORLD_RANK, i);
            char read_descriptor[32];
            snprintf(read_descriptor, sizeof(read_descriptor),
                    "MIMPI_CHANNEL_READ_%d_%d", WORLD_RANK, i);

            write_descriptor[i] = atoi(getenv(write_descriptor));
            read_descriptor[i] = atoi(getenv(read_descriptor));
        }
    }
}

void MIMPI_Finalize() {
    // Zamykamy nie potrzebne deskrytptory
    for (int i = 1; i <= MAX_PROCESSES; i++) {
        close(write_descriptors[i]);
        close(read_descriptors[i]);
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

MIMPI_Retcode MIMPI_Send(void const *data, int count, int destination, int tag){
    TODO
    // Wysyła dane z adresu data interpretujac je jako tablice bajtow wielkosci
    // count do procesu o randze destination.
    // Dodatkowo ta wiadomość jest oznaczana znacznikiem tag, który pozwala na
    // odbiór wiadomosci

    // Jezeli wchodzimy tu i chcemy wysłać wiadomość do procesu do destination
    // ktory juz nie istnieje bo wyszedl z bloku MPI to zwracamy
    // MIMPI_ERROR_REMOTE_FINISHED Chyba tu nie zwracamy MIMPI_DEADLOCK_DETECTED

    // Wysylanie tej wiadomosci pewnie powinno byc robione jako tworzenie chanel
    // miedzy procesami i wpisanie tej wiadomosci do pliku i czekanie na jej
    // odbior przez przyjmujacego

    /*
    if (destination == MIMPI_World_rank()) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    } else if (destination not in MIMPI) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    } else if (destination already finished) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }



    return MIMPI_SUCCESS;
    */

}

MIMPI_Retcode MIMPI_Recv(void *data, int count, int source, int tag){
    TODO
    // Czyka na wiadomość o wielkosci count i znaczniku tag od procesu o randze
    // rank(zapisanej w source)
    // Zapisuje ta wiadomość pod adresem data, nie musimy martwic się ze cos tam
    // nadpiszemy, bo to wołający ma to ogarnac

    // Wywołanie jest BLOKUJACE czyli konczy sie dopiero jak uzyskamy cala
    // wiadomosc Moze do tego jaka tablica semaforow procesoe oczekajacych na
    // wiadomosc, bo jest ich tylko 16 Jezeli proces wychodzi z mimpi to musi
    // powiedziec procesom ktore czekamy na wiadomosc od niego ze wychodzi, bo
    // to moze zakonczyc je błędem.

    // Wersja 1:
    // Jeden proces wysyła komunikaty w tkaiej kolejnsci w jakiej odbiorce chce
    // je odebrac Tzn jezeli wysyłamy z 0 do 1 to nie moze byc tak ze 0 wysyła
    // tag1 a potem tag0, a 1 odbiera tag0 a potem tag1 Jest tez ograniczenie na
    // wielkosc wiadomosci <512 bajtow, bo pewnie ten zjebany channel tylko tyle
    // wysle

    // Wersja 2:
    // Nie mozna nic założyc o kolejnosci wysłanych pakietów

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