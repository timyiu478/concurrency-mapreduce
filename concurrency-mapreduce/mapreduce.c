#include <pthread.h>
#include <semaphore.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "mapreduce.h"

#define DYNAMIC_ARRAY_CAPACITY 64
#define DEBUG 1

typedef struct KVs {
    char *key;
    char **values;
    int size; // number of values for this key
    int capacity; // used to grow the value array
    int index; // used in reducer to track which value to return next; each KVs only reduces once
    sem_t sem; // semaphore to control access to the values or index
} KVs;

typedef struct KVsStore {
    KVs *kvs;
    int size;
    int capacity;
    int index; // used to track the current KVs being processed
    sem_t sem;
} KVsStore;

typedef struct MapperTasks {
    char **file_names; // The file names to be processed by the mapper
    int num_files; // Number of files to be processed by the mapper
    int index; // current index of the file being processed
    sem_t sem; // Semaphore to control access to the file names
} MapperTasks;

KVsStore *partition_stores; // Global variable to hold partition stores so that they can be accessed by the Mapper and Reducer functions.

int global_num_partitions;
Mapper global_map;
Reducer global_reduce;
Partitioner global_partition;

void MapperTasks_Init(MapperTasks *tasks, char **file_names, int num_files) {
    tasks->file_names = file_names;
    tasks->num_files = num_files;
    tasks->index = 0;
    sem_init(&tasks->sem, 0, 1);
}

void MapperTasks_Free(MapperTasks *tasks) {
    for (int i = 0; i < tasks->num_files; i++) {
        free(tasks->file_names[i]);
    }
    free(tasks->file_names);
    sem_destroy(&tasks->sem);
}

char *get_mapper_task(MapperTasks *tasks) {
    sem_wait(&tasks->sem);
    if (tasks->index < tasks->num_files) {
        int tmp = tasks->index;
        tasks->index++;
        sem_post(&tasks->sem);
        return tasks->file_names[tmp];
    }
    sem_post(&tasks->sem);
    return NULL; // No more files to process
}

void *mapper_worker(MapperTasks *tasks) {
    char *file_name = get_mapper_task(tasks);
    while (file_name != NULL) {
        if (DEBUG) {
            printf("Mapper processing file: %s\n", file_name);
        }
        global_map(file_name);
        file_name = get_mapper_task(tasks);
    }
    return NULL;
}

void KVs_Init(KVs *kvs) {
    kvs->key = NULL;
    kvs->values = malloc(DYNAMIC_ARRAY_CAPACITY * sizeof(char *));
    kvs->size = 0;
    kvs->capacity = DYNAMIC_ARRAY_CAPACITY;
    kvs->index = 0;
    sem_init(&kvs->sem, 0, 1);
}

void KVs_Free(KVs *kvs) {
    if (kvs->key) {
        free(kvs->key);
    }
    for (int i = 0; i < kvs->size; i++) {
        free(kvs->values[i]);
    }
    free(kvs->values);
    sem_destroy(&kvs->sem);
}

void KVs_Resize(KVs *kvs) {
    kvs->capacity *= 2;
    kvs->values = realloc(kvs->values, kvs->capacity * sizeof(char *));
}

// Function to get the next value for a key in KVs
char *KVs_GetValue(KVs *kvs) {
    if (DEBUG) {
        printf("KVs_GetValue called for key: %s, current index: %d, size: %d\n", kvs->key, kvs->index, kvs->size);
    }
    if (kvs->index >= kvs->size) {
        return NULL; // Index out of bounds
    }
    sem_wait(&kvs->sem);
    int index = kvs->index;
    kvs->index++;
    sem_post(&kvs->sem);
    return kvs->values[index];
}

void KVsStore_Init(KVsStore *store) {
    store->kvs = malloc(DYNAMIC_ARRAY_CAPACITY * sizeof(KVs));
    store->size = 0;
    store->index = 0;
    store->capacity = DYNAMIC_ARRAY_CAPACITY;
    for (int i = 0; i < DYNAMIC_ARRAY_CAPACITY; i++) {
        KVs_Init(&store->kvs[i]);
    }
    sem_init(&store->sem, 0, 1);
}

void KVsStore_Resize(KVsStore *store) {
    store->capacity *= 2;
    store->kvs = realloc(store->kvs, store->capacity * sizeof(KVs));
    for (int i = store->size; i < store->capacity; i++) {
        KVs_Init(&store->kvs[i]);
    }
}

void KVsStore_Insert(KVsStore *store, char *key, char *value) {
    sem_wait(&store->sem);

    // Check if the key already exists
    for (int i = 0; i < store->size; i++) {
        if (store->kvs[i].key && strcmp(store->kvs[i].key, key) == 0) {
            if (store->kvs[i].size >= store->kvs[i].capacity) {
                KVs_Resize(&store->kvs[i]);
            }
            store->kvs[i].values[store->kvs[i].size] = strdup(value);
            store->kvs[i].size++;
            sem_post(&store->sem);
            if (DEBUG) {
                printf("KVsStore_Insert: Key %s already exists, added value %s\n", key, value);
            }
            return;
        }
    }

    if (store->size >= store->capacity) {
        KVsStore_Resize(store);
    }

    // If the key does not exist, add a new KVs entry
    store->kvs[store->size].key = strdup(key);
    store->kvs[store->size].values[0] = strdup(value);
    store->kvs[store->size].size = 1;
    store->size++;
    sem_post(&store->sem);

    if (DEBUG) {
        printf("KVsStore_Insert: Inserted key %s with value %s\n", key, value);
    }
}

KVs* KVsStore_Lookup(KVsStore *store, char *key) {
    sem_wait(&store->sem);
    for (int i = 0; i < store->size; i++) {
        if (DEBUG) {
            printf("KVsStore_Lookup checking key: %s against %s\n", store->kvs[i].key, key);
        }
        if (store->kvs[i].key && strcmp(store->kvs[i].key, key) == 0) {
            sem_post(&store->sem);
            return &store->kvs[i];
        }
    }
    sem_post(&store->sem);
    return NULL; // Key not found
}

void KVsStore_Free(KVsStore *store) {
    for (int i = 0; i < store->size; i++) {
        KVs_Free(&store->kvs[i]);
    }
    free(store->kvs);
    sem_destroy(&store->sem);
}

char *Reducer_Getter(char *key, int partition_number) {
    if (DEBUG) {
        printf("Reducer_Getter called for key: %s in partition: %d\n", key, partition_number);
    }
    KVsStore *kvs_store = &partition_stores[partition_number];
    KVs *kvs = KVsStore_Lookup(kvs_store, key);
    return KVs_GetValue(kvs);
}

void MR_Emit(char *key, char *value) {
    int partition_number = global_partition(key, global_num_partitions);
    KVsStore_Insert(&partition_stores[partition_number], key, value);
    if (DEBUG) {
        printf("Emitted key: %s, value: %s to partition: %d\n", key, value, partition_number);
    }
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void *reduce_worker(int *partition_number) {
    KVsStore *store = &partition_stores[*partition_number];
    for (int i = 0; i < store->size; i++) {
        KVs *kvs = &store->kvs[i];
        if (kvs->size > 0) {
            // Call the reducer function for each key
            if (DEBUG) {
                printf("Reducer processing key: %s in partition: %d\n", kvs->key, *partition_number);
            }
            global_reduce(kvs->key, Reducer_Getter, *partition_number);
        }
    }
    return NULL;
}

// Comparison function for qsort
int compareKVs(const void *a, const void *b) {
    const KVs *kv1 = (const KVs *)a;
    const KVs *kv2 = (const KVs *)b;

    return strcmp(kv1->key, kv2->key);
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {

    // Set the global variables
    global_map = map;
    global_reduce = reduce;
    global_partition = partition;
    global_num_partitions = num_reducers;

    // Initialize partition stores
    partition_stores = malloc(global_num_partitions * sizeof(KVsStore));
    for (int i = 0; i < global_num_partitions; i++) {
        KVsStore_Init(&partition_stores[i]);
    }

    // Initialize mapper tasks
    MapperTasks tasks;
    char **file_names = malloc((argc - 1) * sizeof(char *));
    for (int i = 1; i < argc; i++) {
        file_names[i - 1] = strdup(argv[i]);
    }
    MapperTasks_Init(&tasks, file_names, argc - 1);

    // Run num_mappers number of mappers in parallel
    pthread_t *mapper_threads = malloc(num_mappers * sizeof(pthread_t));
    for (int i = 0; i < num_mappers; i++) {
        pthread_create(&mapper_threads[i], NULL, (void *)mapper_worker, &tasks);
    }

    // Wait for all mappers to finish
    for (int i = 0; i < num_mappers; i++) {
        pthread_join(mapper_threads[i], NULL);
    }

    // Free mapper tasks & threads
    MapperTasks_Free(&tasks);
    free(mapper_threads);

    // Sorting
    for (int i = 0; i < global_num_partitions; i++) {
        KVsStore *store = &partition_stores[i];
        // Sort the keys in the store
        qsort(store->kvs, store->size, sizeof(KVs), compareKVs);
    }

    // Run reducers
    pthread_t *reducer_threads = malloc(num_mappers * sizeof(pthread_t));
    for (int i = 0; i < num_reducers; i++) {
        pthread_create(&reducer_threads[i], NULL, (void *)reduce_worker, &i);
    }

    // Wait for all reducers to finish
    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reducer_threads[i], NULL);
    }

    // Free reducer threads & partition stores
    free(reducer_threads);
    for (int i = 0; i < global_num_partitions; i++) {
        KVsStore_Free(&partition_stores[i]);
    }

}
