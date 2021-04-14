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

#define BUFSIZE 4096
#define MAX_FILE_LEN 1000
#define MAX_FILES 1000

using namespace std;

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

int parseWords(char *buf, char inputFileNames[MAX_FILES][MAX_FILE_LEN], int inputFileSizes[MAX_FILES], char outputFileName[MAX_FILE_LEN], int *outputFileSz, int *KK)
{
	int currentWordIdx;
	int prevStartIdx;
	int outputFileSize;
	int k=0;	
	int i;
	int prevCharStart=0;

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

	*KK = k;
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

	parseWords(buf, inputFileNames, inputFileSizes, outputFileName, &outputFileSize, &k);

	// dummy placeholder for now, replace by j in future.
	return k;	
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
