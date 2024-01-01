/**
 * This file is for implementation of MIMPI library.
 * */

#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"

#include <stdio.h>
#include <stdlib.h>

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();
    // Tutaj inicjalizacja 'zasobow' potrzebnych do synchronizacji procesow

    //Tutaj trzeba zainicjalizowac worldran i world size
    const char* world_size_env = getenv("MIMPI_WORLD_SIZE");     
    const char* world_rank_env = getenv("MIMPI_WORLD_RANK");     

    if (world_rank_env != NULL && world_size_env != NULL) {
        WORLD_SIZE = atoi(world_size_env);
        WORLD_RANK = atoi(world_rank_env);
    } else {
        fprintf(stderr, "MIMPI_WORLD_SIZE or MIMPI_WORLD_RANK not set!\n");
        exit(EXIT_FAILURE);
    }

    // int start_channel = (WORLD_RANK - 1) * (WORLD_SIZE - 1);
    // for (int j = 1; j <= WORLD_SIZE; j++) {
    //     if (WORLD_RANK != j) { // Pominięcie kanałów między tym samym procesem
    //         int channel_index = start_channel + ((WORLD_RANK < j) ? (j - 1) : j);
    //         if (WORLD_RANK == 1) {
    //             printf("Closing pipe in %d, %d, %d\n", WORLD_RANK, j, channel_index);
    //         }
    //         close(channels[channel_index][0]);
    //         close(channels[channel_index][1]);
    //     }
    // }

    // if (!enable_deadlock_detection) {
    //     // Brak wykrywania zakleszczeń
    // } else {
    //     // Wykrywanie zakleszczeń włączone
    // }
}

void MIMPI_Finalize() {
    // Tutaj usuwamy wszystkie zasoby używane przez program(otwarte pliku,
    // kanały, zaalokowana pamięć i prymitywy synchronizacyjne xd)
    unsetenv("MIMPI_WORLD_SIZE");
    unsetenv("MIMPI_WORLD_RANK");

    channels_finalize();
}

int MIMPI_World_size() {
    return WORLD_SIZE;
}

int MIMPI_World_rank(){
    return WORLD_RANK;
}

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

MIMPI_Retcode MIMPI_Recv(void *data, int count, int source, int tag) {
    TODO
    // Czyka na wiadomość o wielkosci count i znaczniku tag od procesu o randze
    // rank(zapisanej w source)
    // Zapisuje ta wiadomość pod adresem data, nie musimy martwic się ze cos tam
    // nadpiszemy, bo to wołający ma to ogarnac

    // Wywołanie jest BLOKUJACE czyli konczy sie dopiero jak uzyskamy cala wiadomosc
    // Moze do tego jaka tablica semaforow procesoe oczekajacych na wiadomosc, bo jest ich tylko 16
    // Jezeli proces wychodzi z mimpi to musi powiedziec procesom ktore czekamy na wiadomosc od niego 
    // ze wychodzi, bo to moze zakonczyc je błędem.

    // Wersja 1:
    // Jeden proces wysyła komunikaty w tkaiej kolejnsci w jakiej odbiorce chce je odebrac
    // Tzn jezeli wysyłamy z 0 do 1 to nie moze byc tak ze 0 wysyła tag1 a potem tag0,
    // a 1 odbiera tag0 a potem tag1
    // Jest tez ograniczenie na wielkosc wiadomosci <512 bajtow, bo pewnie ten zjebany channel tylko tyle wysle

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
    // Przeprowadza synchronizacje wszystkich procesów? Czyli pewnie działa tak jak zwykła barriera
}

MIMPI_Retcode MIMPI_Bcast(void *data, int count, int root){
    TODO
    // Wysyła dane zapewnione przez proces o randze root do WSZSYTKICH pozostałych procesow
}

MIMPI_Retcode MIMPI_Reduce(void const *send_data, void *recv_data, int count,
                           MIMPI_Op op, int root) {
    TODO
    
}