#include <unistd.h>
#include <error.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
//#include <iostream>
#include <limits.h>
#include <assert.h>
#include <stdint.h>
#include "common.h"

#define BUFSIZE 4096
#define MAX_FILE_LEN 1000
#define MAX_FILES 1000

//using namespace std;

int TOMBSTONE = INT_MIN;

typedef struct KVPair {
	int32_t key;
	int32_t value;
} KVPair_t;

bool isEqual(struct KVPair K1, struct KVPair K2)
{
        return (K1.key == K2.key && K1.value == K2.value);
}

bool isNotEqual(struct KVPair K1, struct KVPair K2)
{
        return (K2.key != K1.key != K2.value != K1.value);
}

bool lessThan(struct KVPair K1, struct KVPair K2)
{
        return K1.key < K2.key;
}

bool greaterThan(struct KVPair K1, struct KVPair K2)
{
        return K1.key > K2.key;
}

typedef struct KVIntPair
{
 	struct KVPair first;
	int second;
} KVIntPair_t;

bool isNotEqualPair(KVIntPair_t K1, KVIntPair_t K2)
{
	return !(isEqual(K1.first, K2.first) && (K1.second == K2.second));
}

bool lessThanPair(KVIntPair_t K1, KVIntPair_t K2)
{
	return lessThan(K1.first,K2.first) || (!(lessThan(K2.first,K1.first)) && K1.second<K2.second);
}

#define LEFTCHILD(x) 2 * x + 1
#define RIGHTCHILD(x) 2 * x + 2
#define PARENT(x) (x - 1) / 2

KVPair_t KVPAIRMAX = (KVPair_t) {INT_MAX, 0};
KVIntPair_t KVINTPAIRMAX; 

int V_TOMBSTONE;

typedef struct StaticHeap {
    int size ;
    KVIntPair_t *arr;
    KVIntPair_t max;
	int i;
/*
    StaticHeap(unsigned sz, KVIntPair_t mx) {
        size = 0;
	arr = (KVIntPair_t *)malloc(sz * sizeof(KVIntPair_t));
        max = mx;
	for (i = 0 ; i < sz; i++) {
		arr[i] = mx;
	}
    }

    void push(KVIntPair_t blob) {
        unsigned i = size++;
        while(i && lessThan(blob, arr[PARENT(i)])) {
            arr[i] = arr[PARENT(i)] ;
            i = PARENT(i) ;
        }
        arr[i] = blob ;
    }
    void heapify(int i) {
        int smallest = (LEFTCHILD(i) < size && lessThan(arr[LEFTCHILD(i)], arr[i])) ? LEFTCHILD(i) : i ;
        if(RIGHTCHILD(i) < size && lessThan(arr[RIGHTCHILD(i)],arr[smallest])) {
            smallest = RIGHTCHILD(i);
        }
        if(smallest != i) {
            KVIntPair_t temp = arr[i];
            arr[i] = arr[smallest];
            arr[smallest] = temp;
            heapify(smallest) ;
        }
    }

    KVIntPair_t pop() {
        KVIntPair_t ret = arr[0];
        arr[0] = arr[--size];
        heapify(0);
        return ret;
    }

	~StaticHeap() {
		free(arr);
	}
	*/
}StaticHeap;

void initStaticHeap(StaticHeap *H, unsigned sz, KVIntPair_t mx)
{
        H->size = 0;
	H->arr = (KVIntPair_t *)malloc(sz * sizeof(KVIntPair_t));
        H->max = mx;
	for (int i = 0 ; i < sz; i++) {
		H->arr[i] = mx;
	}
}

void push(StaticHeap *H, KVIntPair_t blob) {
        unsigned i = H->size++;
        while(i && lessThanPair(blob, H->arr[PARENT(i)])) {
            H->arr[i] = H->arr[PARENT(i)] ;
            i = PARENT(i) ;
        }
        H->arr[i] = blob ;
    }

void heapify(StaticHeap *H, int i) {
        int smallest = (LEFTCHILD(i) < H->size && lessThanPair(H->arr[LEFTCHILD(i)], H->arr[i])) ? LEFTCHILD(i) : i ;
        if(RIGHTCHILD(i) < H->size && lessThanPair(H->arr[RIGHTCHILD(i)],H->arr[smallest])) {
            smallest = RIGHTCHILD(i);
        }
        if(smallest != i) {
            KVIntPair_t temp = H->arr[i];
            H->arr[i] = H->arr[smallest];
            H->arr[smallest] = temp;
            heapify(H, smallest) ;
        }
    }

KVIntPair_t pop(StaticHeap *H) {
        KVIntPair_t ret = H->arr[0];
        H->arr[0] = H->arr[--H->size];
        heapify(H, 0);
        return ret;
}




// currentWordIdx is the first character of the inputFileName
// at the end, currentWordIdx  is set to next first Character or null

int getCurrentWordSize(char *buf, int *currentWordIdx)
{
	int count=0;
	while (buf[*currentWordIdx] != '|') {
		count++;
		*currentWordIdx = *currentWordIdx + 1;
	}
	// skip next |
	*currentWordIdx = *currentWordIdx + 1;
	return count;
}

int getCurrentFileSize(char *buf, int *currentWordIdx)
{
	int number=0;
	while (buf[*currentWordIdx] != '|') {
		number = number * 10 + (buf[*currentWordIdx] - '0');
		*currentWordIdx = *currentWordIdx + 1;
	}
	// skip next |
	*currentWordIdx = *currentWordIdx + 1;
	return number;
}

int parseWords(char *buf, char inputFileNames[MAX_FILES][MAX_FILE_LEN], size_t inputFileSizes[MAX_FILES], char outputFileName[MAX_FILE_LEN], size_t *outputFileSz, int *KK, bool *lastLvl)
{
	int currentWordIdx;
	int prevStartIdx;
	size_t outputFileSize;
	int k=0;	
	int i=0;
	int prevCharStart=0;
	bool lastLevel;

	// first, compute K.
	while (buf[i] != '|') {
		k = (k * 10) + (buf[i]-'0');
		i++;
	}
	
//	printf("k = %d\n", k);
	// skip |
	i++;

	for (currentWordIdx = 0 ; currentWordIdx < k ; currentWordIdx++)
	{
	//	printf("currentWordIdx %d\n" , currentWordIdx);
		prevStartIdx = i;
		// get word size
		int currentWordSize = getCurrentWordSize(buf,&i);
		memcpy(inputFileNames[currentWordIdx], buf + prevStartIdx, currentWordSize);

		// get file size
		int currentFileSize = getCurrentFileSize(buf,&i);
		inputFileSizes[currentWordIdx] = currentFileSize;

	//	printf( "inputFileNames %s\n", inputFileNames[currentWordIdx]);
	//	printf( "inputFileSize %lu\n", inputFileSizes[currentWordIdx]);
	}

	prevStartIdx = i;
	int currentWordSize = getCurrentWordSize(buf,&i);
	memcpy(outputFileName, buf + prevStartIdx, currentWordSize);
	outputFileName[currentWordSize] = '\0';

	outputFileSize = getCurrentFileSize(buf,&i);
	*outputFileSz = outputFileSize;
//	printf( "outputFileName %s\n", outputFileName);
//	printf( "outputFileSize %lu\n", outputFileSize);

	lastLevel = getCurrentFileSize(buf,&i);

	*KK = k;
	*lastLvl = lastLevel == 0 ? false : true;
}

KVPair_t *init_map(const char *filename, size_t filesize)
{
        rdtsc();
        startc = ( ((uint64_t)cycles_high << 32) | cycles_low );

        KVPair_t *map = (KVPair_t *) malloc(filesize);
        if (map == NULL) {
                printf("Could not initialize memory ");
                exit(EXIT_FAILURE);
        }
        int fd = open(filename, O_RDONLY);
        if (fd < 0) {
                printf("%s:%d:Open failed on write %s %s\n", __FILE__, __LINE__, strerror(errno), filename);
                exit(1);
        }
        int ret = read(fd, map, filesize);
        if (ret != filesize) {
                printf("Read failed\n");
                exit(1);
        }
        ret = close(fd);

        rdtsc();
        endc = ( ((uint64_t)cycles_high << 32) | cycles_low ); 
        computation_fsync_time += (endc - startc);  

        return map;
}

void exitMap(KVPair_t *map, char *_filename, size_t filesize)
{
        rdtsc();
        startc = ( ((uint64_t)cycles_high << 32) | cycles_low );
        int fd = open(_filename, O_WRONLY);
        if (fd < 0) {
                printf("%s:%d:Open failed on write %s %s\n", __FILE__, __LINE__, strerror(errno), _filename);
                exit(1);
        }

        int ret = write(fd, map, filesize);
        if (ret != filesize) {
                printf("Write Failed\n");
                exit(1);
        }

        ret = fsync(fd);
        if (ret < 0) {
                printf("Sync failed\n");
                exit(1);
        }

        ret = close(fd);
        if (ret < 0) {
                printf("Close failed\n");
                exit(1);
        }

        rdtsc();
        endc = ( ((uint64_t)cycles_high << 32) | cycles_low ); 
        computation_fsync_time += (endc - startc);  
}

int addRunsCompute(int k, char inputFileNames[MAX_FILES][MAX_FILE_LEN], size_t inputFileSizes[MAX_FILES], char outputFileName[MAX_FILE_LEN], size_t outputFileSize , bool lastLevel) 
{
	KVPair_t **input_maps = (KVPair_t **)malloc(k * (sizeof(KVPair_t *)));
        KVPair_t * output_map;
        int compute_j;
	int *heads = (int *)malloc(sizeof(int) * k);

        // move this to compute.
        int num_ip_files = k;

        for (int curr = 0; curr < num_ip_files ; curr++)
        {
                input_maps[curr] = init_map(inputFileNames[curr],inputFileSizes[curr]);
		heads[curr]=0;
        }

        output_map = init_map(outputFileName, outputFileSize);

        //StaticHeap h = StaticHeap((int) k, KVINTPAIRMAX);
        StaticHeap h;
	initStaticHeap (&h, k, KVINTPAIRMAX);

        for (int i = 0; i < k; i++){
            KVPair_t kvp;
                memcpy(&kvp,input_maps[i], sizeof(KVPair_t));
		push(&h, (KVIntPair_t){kvp, i});
        }

        int j = -1;
	// TODO change int type to new datatype on a different key type
//        K lastKey = INT_MAX;
        int32_t lastKey = INT_MAX;
        unsigned lastk = INT_MIN;
        while (h.size != 0){
            KVIntPair_t val_run_pair = pop(&h);
            assert(isNotEqualPair(val_run_pair, KVINTPAIRMAX)); // TODO delete asserts
            if (lastKey == val_run_pair.first.key){
                if( lastk < val_run_pair.second){
                    memcpy(output_map + j, &val_run_pair.first, sizeof(KVPair_t));
                }
            }
            else {
                ++j;
                KVPair_t tmp;
                if (j!= -1) {
                        memcpy(&tmp, output_map + j , sizeof(KVPair_t));
                }
                if ( j != -1 && lastLevel && tmp.value == V_TOMBSTONE){
                    --j;
                }
                memcpy(output_map + j, &val_run_pair.first, sizeof(KVPair_t));
//                output_map[j] = val_run_pair.first;
            }

            lastKey = val_run_pair.first.key;
            lastk = val_run_pair.second;

            unsigned k = val_run_pair.second;
            if (++heads[k] < inputFileSizes[k] / sizeof(KVPair_t)){
   //             KVPair_t kvp = input_maps[k][heads[k]];
		KVPair_t kvp;
                memcpy(&kvp, input_maps[k] + heads[k], sizeof(KVPair_t));
                push(&h,(KVIntPair_t){kvp, k});
            }
        }

        KVPair_t tmp;
        memcpy(&tmp, output_map + j , sizeof(KVPair_t));
        if (lastLevel && tmp.value == V_TOMBSTONE){
            --j;
        }
        // return back from compute.

        // j = compute_j;
        /*

        move these two functions to lsm.hpp as we want to not port
        fence pointer logic to compute process at the moment.
        TODO: revisit this design decision.
        runs[_activeRun]->setCapacity(j + 1);
        runs[_activeRun]->constructIndex();
        */

        /*_prevRun = _activeRun;

        if(j + 1 > 0){
            ++_activeRun;
        }
	*/
        // write output data back to disk
        exitMap(output_map, outputFileName, outputFileSize);
        // input maps were read only. they are freed by call to distructor
	for (int i = 0; i < k ; i++) {
		free(input_maps[i]);
	}
	free(input_maps);
	free(heads);
	free(output_map);
	free(h.arr);
        return j;
}

int compute(char *buf, int read_bytes)
{
	// disk receives a single string of data in the format.
	// K|InputFile1|InputFile1Size|InputFile2|InputFile2Size ... |InputFileK|InputFileKSize|OutputFile|OutputFileSize|
	// we parse the buffer into an array of multiple files

	char inputFileNames[MAX_FILES][MAX_FILE_LEN];
	size_t inputFileSizes[MAX_FILES];
	char outputFileName[MAX_FILE_LEN];
	size_t outputFileSize;
	int k;
	int currentWordIdx;
	bool lastLevel;

	parseWords(buf, inputFileNames, inputFileSizes, outputFileName, &outputFileSize, &k, &lastLevel);

	int j = addRunsCompute(k, inputFileNames, inputFileSizes, outputFileName, outputFileSize, lastLevel);

	// dummy placeholder for now, replace by j in future.
	return j;
}

int main(int argc, char *argv[])
{


	const char *app_reader = "/tmp/app_reader";
	const char *app_writer = "/tmp/app_writer";

	int read_fd, write_fd;
	char buf[BUFSIZE] = {'\n'};
	int count = 0;
	int j;

	KVINTPAIRMAX.first = KVPAIRMAX;
	KVINTPAIRMAX.second = -1;
	V_TOMBSTONE = (int) TOMBSTONE;

	if( !access( app_writer, F_OK ) == 0 )
	{
		mkfifo(app_writer, 0666);
	}

	if( !access( app_reader, F_OK ) == 0 )
	{
		mkfifo(app_reader, 0666);
	}

	read_fd = open(app_writer, O_RDONLY);
	if (read_fd < 0) {
		printf("%s:Could not open %s for reading %s\n", argv[0], app_writer, strerror(errno));
		exit(1);
	}

//	printf("opened read_fd\n");
	// write computation output
	write_fd = open(app_reader, O_WRONLY);
	if (write_fd < 0) {
		printf("%s:Could not open %s for writing %s\n", argv[0], app_reader, strerror(errno));
		exit(1);
	}

//	printf("opened write_fd\n");

	// read application data
	while (1)
	{
		//printf("Start Read\n");
		
		int read_bytes = 0;
		read_bytes = read(read_fd, buf, BUFSIZE);
		if (read_bytes <= 0) {
			printf("done reading: %s\n", strerror(errno));
			break;
		}

//		printf("read bytes %d\n", read_bytes);
		//printf("All Reads Complete\n");
		j = compute(buf, read_bytes);
	
//		printf("computation complete, writing output \n");
		count = j;
		int result = write(write_fd, &count, sizeof(int));
		if (result < 0) {
			printf("%s:Could not write %s\n", argv[0], strerror(errno));
			exit(1);
		}
	}

	close(read_fd);
	close(write_fd);

	printf("done\n");
    printf("Computation Fsync Time %lu\n", computation_fsync_time);
}
