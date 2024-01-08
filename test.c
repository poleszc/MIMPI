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

typedef union unionInitialDataChunk {
    struct {
        int source;
        int count;
        int tag;
    } values;
    char data[CHUNK_SIZE];
} unionInitialDataChunk;

void printMemory(void *ptr, size_t numBytes) {
    fprintf(stderr, "PRINT MEMORY\n");
    unsigned char *bytePtr = (unsigned char *)ptr; // Rzutowanie wskaźnika na bajty

    for (size_t i = 0; i < numBytes; i++) {
        if(i%32 == 0) {
            fprintf(stderr, "\n");
        }
        fprintf(stderr, "%02X ", bytePtr[i]); // Wypisanie wartości w formacie szesnastkowym
    }
    fprintf(stderr, "\n");
}

int main() {
    char* zero = (char*)calloc(CHUNK_SIZE, sizeof(char));

git 
    unionInitialDataChunk chunk;
    chunk.values.source = 1;
    chunk.values.count = 2;
    chunk.values.tag = 3;
    memcpy(chunk.data, zero, CHUNK_SIZE - 3 * sizeof(int));

    printMemory(&chunk, sizeof(unionInitialDataChunk));

    return 0;

}