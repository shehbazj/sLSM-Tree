#include <unistd.h>
#include <error.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <cstdint>
#include <limits>
#include <vector>
#include <cassert>

#define BUFSIZE 4096
#define MAX_FILE_LEN 1000
#define MAX_FILES 1000

using namespace std;

int TOMBSTONE = numeric_limits<std::int32_t>::min();

typedef struct KVPair {
	int key;
	int value;
} KVPair_t;

typedef struct KVIntPair {
	int key;
	int value;
	int second_field;

	KVIntPair() : key(numeric_limits<int>::max()), value(numeric_limits<int>::max()), second_field(numeric_limits<int>::max())
	{
	}

	KVIntPair(KVPair_t pair, int second)
	{
		key = pair.key;
		value = pair.value;
		second_field = second;
	}
} KVIntPair_t;

#define LEFTCHILD(x) 2 * x + 1
#define RIGHTCHILD(x) 2 * x + 2
#define PARENT(x) (x - 1) / 2

KVPair_t KVPAIRMAX;
KVIntPair_t KVINTPAIRMAX;

int V_TOMBSTONE = (int) TOMBSTONE;

struct StaticHeap {
    int size ;
    vector<KVIntPair_t> arr;
    KVIntPair_t max;

    StaticHeap(unsigned sz, KVIntPair_t mx) {
        size = 0;
        arr = vector<KVIntPair_t>(sz, mx);
        max = mx;
    }

    void push(KVIntPair_t blob) {
        unsigned i = size++;
        while(i && blob.key < arr[PARENT(i)].key) {
            arr[i] = arr[PARENT(i)] ;
            i = PARENT(i) ;
        }
        arr[i] = blob ;
    }
    void heapify(int i) {
        int smallest = (LEFTCHILD(i) < size && arr[LEFTCHILD(i)].key < arr[i].key) ? LEFTCHILD(i) : i ;
        if(RIGHTCHILD(i) < size && arr[RIGHTCHILD(i)].key < arr[smallest].key) {
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
};

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

int parseWords(char *buf, char inputFileNames[MAX_FILES][MAX_FILE_LEN], int inputFileSizes[MAX_FILES], char outputFileName[MAX_FILE_LEN], int *outputFileSz, int *KK, bool *lastLvl)
{
	int currentWordIdx;
	int prevStartIdx;
	int outputFileSize;
	int k=0;	
	int i;
	int prevCharStart=0;
	bool lastLevel;

	// first, compute K.
	while (buf[i] != '|') {
		k = (k * 10) + (buf[i]-'0');
		i++;
	}
	
	cout << "K is  " << k << endl;
	// skip |
	i++;

	for (currentWordIdx = 0 ; currentWordIdx < k ; currentWordIdx++)
	{
		cout << "currentWordIdx " << currentWordIdx << endl;
		prevStartIdx = i;
		// get word size
		int currentWordSize = getCurrentWordSize(buf,&i);
		memcpy(inputFileNames[currentWordIdx], buf + prevStartIdx, currentWordSize);

		// get file size
		int currentFileSize = getCurrentFileSize(buf,&i);
		inputFileSizes[currentWordIdx] = currentFileSize;

		cout << "inputFileNames " << inputFileNames[currentWordIdx] << endl;
		cout << "inputFileSize " << inputFileSizes[currentWordIdx] << endl;
	}

	prevStartIdx = i;
	int currentWordSize = getCurrentWordSize(buf,&i);
	memcpy(outputFileName, buf + prevStartIdx, currentWordSize);
	outputFileName[currentWordSize] = '\0';

	outputFileSize = getCurrentFileSize(buf,&i);
	*outputFileSz = outputFileSize;
	cout << "outputFileName " << outputFileName << endl;
	cout << "outputFileSize " << outputFileSize << endl;

	lastLevel = getCurrentFileSize(buf,&i);

	*KK = k;
	*lastLvl = lastLevel == 0 ? false : true;
}

KVPair_t *init_map(const char *filename, size_t filesize)
{
        KVPair_t *map = (KVPair_t *) (new uint8_t[filesize]);
        if (map == nullptr) {
                cout << "Could not initialize memory " << endl;
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
        return map;
}

void exitMap(KVPair_t *map, char *_filename, size_t filesize)
{
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
}

int addRunsCompute(int k, char inputFileNames[MAX_FILES][MAX_FILE_LEN], int inputFileSizes[MAX_FILES], char outputFileName[MAX_FILE_LEN], int outputFileSize , bool lastLevel) 
{
	 vector <KVPair_t *> input_maps;
        KVPair_t * output_map;
        int compute_j;

        // move this to compute.
        int num_ip_files = k;

        for (int curr = 0; curr < num_ip_files ; curr++)
        {
                input_maps.push_back(init_map(inputFileNames[curr],inputFileSizes[curr]));
        }

        output_map = init_map(outputFileName, outputFileSize);

        StaticHeap h = StaticHeap((int) k, KVINTPAIRMAX);
        vector<int> heads(k, 0);
        for (int i = 0; i < k; i++){
            KVPair_t kvp;
                memcpy(&kvp,input_maps[i], sizeof(KVPair_t));
		h.push(KVIntPair_t(kvp, i));
        }

        int j = -1;
	// TODO change int type to new datatype on a different key type
//        K lastKey = INT_MAX;
        int lastKey = numeric_limits<int>::max();
        unsigned lastk = numeric_limits<int> :: min();
        while (h.size != 0){
            auto val_run_pair = h.pop();
            assert(val_run_pair.key != KVINTPAIRMAX.key); // TODO delete asserts
            if (lastKey == val_run_pair.key){
                if( lastk < val_run_pair.second_field){
			KVPair_t kvp;
			kvp.key = val_run_pair.key;
			kvp.value = val_run_pair.value;
                    memcpy(output_map + j, &kvp, sizeof(KVPair_t));
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
		KVPair_t kvp;
		kvp.key = val_run_pair.key;
		kvp.value = val_run_pair.value;
                memcpy(output_map + j, &kvp, sizeof(KVPair_t));
//                output_map[j] = val_run_pair.first;
            }

            lastKey = val_run_pair.key;
            lastk = val_run_pair.value;

            unsigned k = val_run_pair.second_field;
            if (++heads[k] < inputFileSizes[k] / sizeof(KVPair_t)){
   //             KVPair_t kvp = input_maps[k][heads[k]];
                KVPair_t kvp;
                memcpy(&kvp, input_maps[k] + heads[k], sizeof(KVPair_t));
                h.push(KVIntPair_t(kvp, k));
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
        return j;
}

int compute(char *buf, int read_bytes)
{
	// disk receives a single string of data in the format.
	// K|InputFile1|InputFile1Size|InputFile2|InputFile2Size ... |InputFileK|InputFileKSize|OutputFile|OutputFileSize|
	// we parse the buffer into an array of multiple files

	char inputFileNames[MAX_FILES][MAX_FILE_LEN];
	int inputFileSizes[MAX_FILES];
	char outputFileName[MAX_FILE_LEN];
	int outputFileSize;
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

	printf("opened read_fd\n");
	// write computation output
	write_fd = open(app_reader, O_WRONLY);
	if (write_fd < 0) {
		printf("%s:Could not open %s for writing %s\n", argv[0], app_reader, strerror(errno));
		exit(1);
	}

	printf("opened write_fd\n");

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

		printf("read bytes %d\n", read_bytes);
		//printf("All Reads Complete\n");
		j = compute(buf, read_bytes);
	
		printf("computation complete, writing output \n");
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
}

/*
	Test Harness to test parsing code.

	char buf[4096];
	const char *input = "3|InputFile1|10|Input2|200|IP3|3000|OutputFile|25|";

	strcpy(buf, input);
		
	compute(buf, 100);
*/
