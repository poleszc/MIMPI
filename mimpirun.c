#include "channel.h"
#include "mimpi_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define MIN_PROCESSES 1
#define MAX_PROCESSES 16
#define MIN_USABLE_DESCRIPTOR_NUMBER 20
#define MAX_USABLE_DESCRIPTOR_NUMBER 1023

#define WORLD_SIZE_ENV "MIMPI_WORLD_SIZE"
#define WORLD_RANK_ENV "MIMPI_WORLD_RANK"
#define CHANNEL_READ_ENV "MIMPI_CHANNEL_READ"

// Jeżeli wywołujemy MIMPI_Recv będąc procesem o mniejszym indeksie niż ten do którego wysyłamy
// To prosimy go o pozwolenie. Tj. wysyłamy mu wiadomość, że chcemy na niego wywołać recv. Jeżeli on ma zapisane
// w tablicy ze juz ma wywołane na nas recv to budzi swojego zeby sie wywalił oraz odsyła wiadomosc ze nie mozna
// i masz zwrocić bład. Jeżeli wiekszy nie wywołał recv na tamten to odsyła ze spoko i wtedy zaznacza sobie ze
// zostało na niego wywołąne recv wiec jak on teraz wywoła recv to wie ze ma sie wywalić.

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

    // Chwilowo tworzymy deskryptory dopóki nie stworzymy wiekszego niz 19, zeby
    // nasze zaczynały się od 20
    int tempDescriptors[MIN_USABLE_DESCRIPTOR_NUMBER];
    int mockDesc[2];
    ASSERT_SYS_OK(channel(mockDesc));
    int currDesc = mockDesc[0];

    int numOfOpenDescriptors = 0;
    while (currDesc <= MIN_USABLE_DESCRIPTOR_NUMBER - 1 && numOfOpenDescriptors < 20) {
        currDesc = dup(mockDesc[0]);
        tempDescriptors[numOfOpenDescriptors] = currDesc;
        numOfOpenDescriptors++;
    }

    // Jak wyjdziemy tutaj to currDesc to minimalnie 19, wiec mozna zaczać
    // tworzyć te odpowiednie deskryptor
    int channels[MAX_PROCESSES][2];
    for (int i = 0; i < n; i++) {
        ASSERT_SYS_OK(channel(channels[i]));
    }

    // Zamykamy tymczasowe deskryptory
    close(mockDesc[0]);
    close(mockDesc[1]);
    for (int i = 0; i < numOfOpenDescriptors; i++) {
        ASSERT_SYS_OK(close(tempDescriptors[i]));
    }

    // Ustawiamy zmienna środowiskowa oznaczjąca liczbę odpalonych równolegle
    // programów
    char size_arg[3];
    snprintf(size_arg, sizeof(size_arg), "%d", n);
    setenv(WORLD_SIZE_ENV, size_arg, 1);

    char *prog = argv[2];         // nazwa prog
    char **args = &argv[2];       // dodatkowe argumenty do prog
    for (int i = 0; i < n; i++) { // Odpalamy n procesow

        pid_t pid;
        ASSERT_SYS_OK(pid = fork());
        if (pid == 0) { // Proces potomny
            // Ustawiamy zmienna oznaczająca indeks programu
            char index_arg[4];
            snprintf(index_arg, sizeof(index_arg), "%d", i);
            setenv(WORLD_RANK_ENV, index_arg, 1);

            char readDesc[4];
            snprintf(readDesc, sizeof(readDesc), "%d", channels[i][0]);
            setenv(CHANNEL_READ_ENV, readDesc, 1);

            // TODO: Close not used in process descriptors

            for (int i = 0; i < n; i++) {
                char writeDescEnv[32];
                snprintf(writeDescEnv, sizeof(writeDescEnv), "MIMPI_CHANNEL_WRITE_%d", i);
                // TODO: Make the buffer smaller
                char writeDesc[4];
                snprintf(writeDesc, sizeof(writeDesc), "%d", channels[i][1]);
                setenv(writeDescEnv, writeDesc, 1);
            }

            ASSERT_SYS_OK(execvp(prog, args));
        }
    }

    // Czekanie na zakończenie wszystkich procesów potomnych
    for (int i = 0; i < n; ++i) {
        ASSERT_SYS_OK(wait(NULL));
    }

    // Zamykamy używane deskryptory
    for (int i = 0; i < n; i++) {
        close(channels[i][0]);
        close(channels[i][1]);
    }

    return 0;
}