#include "channel.h"
#include "mimpi_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define MIN_PROCESSES 1
#define MAX_PROCESSES 16
#define STARTING_DESCRIPTOR_NUMBER 60
#define MIN_USABLE_DESCRIPTOR_NUMBER 20
#define MAX_USABLE_DESCRIPTOR_NUMBER 1023
// #define MAX_CHANNELS 257
extern char **environ;

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Not enough arguments!\n");
        return -1;
    }

    int n = atoi(argv[1]);
    if (n < MIN_PROCESSES || n > MAX_PROCESSES) {
        fprintf(stderr, "Wrong number of prog copies to start!\n");
        return -1;
    }

    // Ustawiamy zmienna środowiskowa oznaczjąca liczbę odpalonych równolegle
    // programów
    char size_arg[3];
    snprintf(size_arg, sizeof(size_arg), "%d", n);
    setenv("MIMPI_WORLD_SIZE", size_arg, 1);

    // Chwilowo tworzymy deskryptory dopóki nie stworzymy wiekszego niz 19, zeby
    // nasze zaczynały się od 20
    int tempDescriptors[MIN_USABLE_DESCRIPTOR_NUMBER];
    int mockDesc[2];
    ASSERT_SYS_OK(channel(mockDesc));
    int currDesc = mockDesc[0];

    int numOfOpenDescriptors = 0;
    while (currDesc <= MIN_USABLE_DESCRIPTOR_NUMBER - 1 &&
           numOfOpenDescriptors < 20) {
        currDesc = dup(mockDesc[0]);
        // printf("curr in loop: %d\n", currDesc);
        tempDescriptors[numOfOpenDescriptors] = currDesc;
        numOfOpenDescriptors++;
    }

    // Jak wyjdziemy tutaj to currDesc to minimalnie 19, wiec mozna zaczać
    // tworzyć te odpowiednie deskryptor
    int channels[MAX_PROCESSES][MAX_PROCESSES][2];
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            if (i != j) {
                ASSERT_SYS_OK(channel(channels[i][j]));
            }
        }
    }

    printf("===========PRINT ALL CHANNELS===========\n");
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            printf("{%d, %d} | ", channels[i][j][0], channels[i][j][1]);
        }
        printf("\n");
    }
    printf("=========================================\n");

    // Zamykamy tymczasowe deskryptory
    close(mockDesc[0]);
    close(mockDesc[1]);
    for (int i = 0; i < numOfOpenDescriptors; i++) {
        ASSERT_SYS_OK(close(tempDescriptors[i]));
    }

    char *prog = argv[2];         // nazwa prog
    char **args = &argv[3];       // dodatkowe argumenty do prog
    for (int i = 0; i < n; i++) { // Odpalamy n procesow
        pid_t pid;
        ASSERT_SYS_OK(pid = fork());
        if (pid == 0) { // Proces potomny
            // Ustawiamy zmienna oznaczająca indeks programu
            char index_arg[3];
            snprintf(index_arg, sizeof(index_arg), "%d", i);
            setenv("MIMPI_WORLD_RANK", index_arg, 1);

            // Przekazujemy dekstyptory do czytania i pisania do programu
            int descriptor_count = 0;
            for (int j = 0; j < n; j++) {
                if (i != j && descriptor_count < 15) {
                    char write_descriptor[32];
                    snprintf(write_descriptor, sizeof(write_descriptor),
                             "MIMPI_CHANNEL_WRITE_%d", j + 1);
                    char read_descriptor[32];
                    snprintf(read_descriptor, sizeof(read_descriptor),
                             "MIMPI_CHANNEL_READ_%d", j + 1);

                    char write_desc[8], read_desc[8];
                    snprintf(write_desc, sizeof(write_desc), "%d",
                             channels[i][j][1]);
                    snprintf(read_desc, sizeof(read_desc), "%d",
                             channels[j][i][0]);

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