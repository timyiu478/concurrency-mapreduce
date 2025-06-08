#include <pthread.h>
#include <stdlib.h>

#include "mapreduce.h"

typedef struct KVs {
    char *key;
    char **value;
    int size; // number of values for this key
    int capacity; // used to grow the value array
    int index; // used in reducer to track which value to return next; each KVs only reduces once
} KVs;

typedef struct KVsStore {
    KVs *kvs;
    pthread_semaphore key_sem;
} KVsStore;

void KVsStore_Init(KeyValuesStore *store) {
}

void KVsStore_Insert(KeyValuesStore *store, char *key, char *value) {
}

void KVsStore_Lookup(KeyValuesStore *store) {
}

void KVsStore_Free(KeyValuesStore *store) {
}


void MR_Emit(char *key, char *value) {

}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {


}
