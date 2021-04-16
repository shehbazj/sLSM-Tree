all: sLSM computation

sLSM:
	g++ -g -fpermissive -std=c++11 main.cpp MurmurHash.cpp -o main.out -lpthread

computation: computation.c
	gcc -g computation.c -o computation

clean:
	rm -rf computation main.out
