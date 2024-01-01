/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>


int main(int argc, char *argv[]) {
    /* 
        Programy tylko raz wchodza do bloku MPI i bedac w nim moga sie miedzy
        soba komunikowac. Wiemy ze jest ich tylko 16 wiec mozna zrobic 256 deskryptorów, 
        do komunikacji miedzy nimi
    */

    if (argc < 3) {
        fprintf(stderr, "Not enough arguments!\n");
        return -1;
    }

    int n = atoi(argv[1]);
    if (n < 1 || n > 16) {
        fprintf(stderr, "Wrong number of prog copies to start!\n");
        return -1;
    }

    // TODO: PRZYGOTUJ ŚRODOWISKO
    char *prog = argv[2];   // nazwa prog
    char **args = &argv[3]; // dodatkowe argumenty do prog

    // Ustawiamy zmienna środowiskowa oznaczjąca liczbę odpalonych równolegle programów
    char size_arg[3];
    snprintf(size_arg, sizeof(size_arg), "%d", n);
    setenv("MIMPI_WORLD_SIZE", size_arg, 1);

    for (int i = 1; i <= n; ++i) { // Odpalamy n procesow
        pid_t pid;
        ASSERT_SYS_OK(pid = fork());

        if (pid == 0) { // Proces potomny
            // Ustawiamy zmienna oznaczająca indeks programu
            char index_arg[3];
            snprintf(index_arg, sizeof(index_arg), "%d", i);
            setenv("MIMPI_WORLD_RANK", index_arg, 1);

            ASSERT_SYS_OK(execvp(prog, args));
        }
    }

    // Czekanie na zakończenie wszystkich procesów potomnych
    for (int i = 0; i < n; ++i) {
        ASSERT_SYS_OK(wait(NULL));
    }
    // TODO: ZAKONCZ DZIALANIE I ZWOLNIJ EWENTUALNIE UZYWANA PAMIEC

    return 0;
}