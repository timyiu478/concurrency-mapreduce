# Concurrency Mapreduce

## Map Reduce Background

In 2004, engineers at Google introduced a new paradigm for large-scale parallel data processing known as MapReduce (see the original paper [here](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf), and make sure to look in the citations at the end). One key aspect of MapReduce is that it makes programming such tasks on large-scale clusters easy for developers; instead of worrying about how to manage parallelism, handle machine crashes, and many other complexities common within clusters of machines, the developer can instead just focus on writing little bits of code (described below) and the infrastructure handles the rest.

## Example Usage

To run the MapReduce example, you can use the following command:

```bash
./mapreduce test_files/3/in/*
jumps 10
 10
dog 10
brown 10
quick 10
the 20
fox 10
lazy 10
over 10
```

MapReduce Infrastructure setting: 10 mappers, 10 reducers, and default hash partitioning.

```c
int main(int argc, char *argv[]) {
    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
}
```

## Implementation Limitations

- does not support running multiple MapReduce jobs in parallel.
- does not support running workers on multiple machines.
- does not support *O(1)* time key lookups.
