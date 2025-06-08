all: main.o mapreduce.o mapreduce.h
	gcc -g -o mapreduce mapreduce.o main.o -Wall -Werror -O -pthread

mapreduce.o: mapreduce.c mapreduce.h
	gcc -g -c -o mapreduce.o mapreduce.c -Wall -Werror -O -pthread

main.o: main.c
	gcc -g -c main.c -Wall -Werror -O -pthread

clean:
	rm *.o mapreduce
