#include "channel.h"
#include "mimpi_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define MIN_PROCESSES 1
#define MAX_PROCESSES 16
// #define MAX_CHANNELS 257

int main(int argc, char *argv[]) {
    /*
        Programy tylko raz wchodza do bloku MPI i bedac w nim moga sie miedzy
        soba komunikowac. Wiemy ze jest ich tylko 16 wiec mozna zrobic 256
       deskryptorów, do komunikacji miedzy nimi
    */

    if (argc < 3) {
        fprintf(stderr, "Not enough arguments!\n");
        return -1;
    }

    int n = atoi(argv[1]);
    if (n < MIN_PROCESSES || n > MAX_PROCESSES) {
        fprintf(stderr, "Wrong number of prog copies to start!\n");
        return -1;
    }

    // TODO: PRZYGOTUJ ŚRODOWISKO
    char *prog = argv[2];   // nazwa prog
    char **args = &argv[3]; // dodatkowe argumenty do prog

    // Ustawiamy zmienna środowiskowa oznaczjąca liczbę odpalonych równolegle
    // programów
    char size_arg[3];
    snprintf(size_arg, sizeof(size_arg), "%d", n);
    setenv("MIMPI_WORLD_SIZE", size_arg, 1);

    int channels[MAX_PROCESSES][MAX_PROCESSES][2];

    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            if (i != j) {
                ASSERT_SYS_OK(channel(channels[i][j]));
            }
        }
    }

    for (int i = 0; i < n; i++) { // Odpalamy n procesow
        pid_t pid;
        ASSERT_SYS_OK(pid = fork());

        if (pid == 0) { // Proces potomny
            // Ustawiamy zmienna oznaczająca indeks programu
            char index_arg[3];
            snprintf(index_arg, sizeof(index_arg), "%d", i+1);
            setenv("MIMPI_WORLD_RANK", index_arg, 1);

            // Przekazujemy dekstyptory do czytania i pisania do programu
            int descriptor_count = 0;
            for (int j = 0; j < n; ++j) {
                if (i != j && descriptor_count < 15) {
                    char write_descriptor[32];
                    snprintf(write_descriptor, sizeof(write_descriptor), "MIMPI_CHANNEL_WRITE_%d_%d", i, j);
                    char read_descriptor[32];
                    snprintf(read_descriptor, sizeof(read_descriptor), "MIMPI_CHANNEL_READ_%d_%d", i, j);

                    char write_desc[5], read_desc[5];
                    snprintf(write_desc, sizeof(write_desc), "%d", channels[i][j][1]);
                    snprintf(read_desc, sizeof(read_desc), "%d", channels[j][i][0]);

                    setenv(write_descriptor, write_desc, 1);
                    setenv(read_descriptor, read_desc, 1);
                    descriptor_count++;
                }
            }

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