//
//  diskLevel.hpp
//  lsm-tree
//
//    sLSM: Skiplist-Based LSM Tree
//    Copyright © 2017 Aron Szanto. All rights reserved.
//
//    This program is free software: you can redistribute it and/or modify
//    it under the terms of the GNU General Public License as published by
//    the Free Software Foundation, either version 3 of the License, or
//    (at your option) any later version.
//
//    This program is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//    GNU General Public License for more details.
//
//        You should have received a copy of the GNU General Public License
//        along with this program.  If not, see <http://www.gnu.org/licenses/>.
//
#pragma once

#ifndef diskLevel_h
#define diskLevel_h
#include <vector>
#include <cstdint>
#include <string>
#include <cstring>
#include "run.hpp"
#include "diskRun.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <cassert>
#include <algorithm>

#define LEFTCHILD(x) 2 * x + 1
#define RIGHTCHILD(x) 2 * x + 2
#define PARENT(x) (x - 1) / 2

int TOMBSTONE = INT_MIN;

using namespace std;


template <class K, class V>
class DiskLevel {
    
public: // TODO make some of these private
    typedef KVPair<K,V> KVPair_t;
    typedef pair<KVPair<K, V>, int> KVIntPair_t;
    KVPair_t KVPAIRMAX;
    KVIntPair_t KVINTPAIRMAX;
    V V_TOMBSTONE = (V) TOMBSTONE;

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
            while(i && blob < arr[PARENT(i)]) {
                arr[i] = arr[PARENT(i)] ;
                i = PARENT(i) ;
            }
            arr[i] = blob ;
        }
        void heapify(int i) {
            int smallest = (LEFTCHILD(i) < size && arr[LEFTCHILD(i)] < arr[i]) ? LEFTCHILD(i) : i ;
            if(RIGHTCHILD(i) < size && arr[RIGHTCHILD(i)] < arr[smallest]) {
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

    
    
    
    
    
    
    int _level;
    unsigned _pageSize; // number of elements per fence pointer
    unsigned long _runSize; // number of elts in a run
    unsigned _numRuns; // number of runs in a level
    unsigned _activeRun; // index of active run
    unsigned _prevRun; // index of active run
    unsigned _mergeSize; // # of runs to merge downwards
    double _bf_fp; // bloom filter false positive
    vector<DiskRun<K,V> *> runs;
	int read_fd, write_fd; // computational storage file descriptors
    
    
    DiskLevel<K,V>(unsigned int pageSize, int level, unsigned long runSize, unsigned numRuns, unsigned mergeSize, double bf_fp, int read_fd, int write_fd):_numRuns(numRuns), _runSize(runSize),_level(level), _pageSize(pageSize), _mergeSize(mergeSize), _activeRun(0), _bf_fp(bf_fp), read_fd(read_fd), write_fd(write_fd) {
        KVPAIRMAX = (KVPair_t) {INT_MAX, 0};
        KVINTPAIRMAX = KVIntPair_t(KVPAIRMAX, -1);
        
        for (int i = 0; i < _numRuns; i++){
            DiskRun<K,V> * run = new DiskRun<K, V>(_runSize, pageSize, level, i, _bf_fp);
            runs.push_back(run);
        }

        

        
        
    }
    
    ~DiskLevel<K,V>(){
        for (int i = 0; i< runs.size(); ++i){
            delete runs[i];
        }
    }

KVPair_t *init_map(string _filename, size_t filesize) 
{
        KVPair_t *map = (KVPair<K, V>*) (new char[filesize]);
        if (map == nullptr) {
                cout << "Could not initialize memory " << endl;
                exit(EXIT_FAILURE);
        }
        int fd = open(_filename.c_str(), O_RDONLY);
        if (fd < 0) {
                printf("%s:%d:Open failed on write %s %s\n", __FILE__, __LINE__, strerror(errno), _filename.c_str());
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

void exitMap(KVPair_t *map, string _filename, size_t filesize)
{
        int fd = open(_filename.c_str(), O_WRONLY);
        if (fd < 0) {
                printf("%s:%d:Open failed on write %s %s\n", __FILE__, __LINE__, strerror(errno), _filename.c_str());
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

	string createString(vector <string> &inputFileNames, vector <size_t> inputFileSizes, string outputFileName, size_t outputFileSize, bool lastLevel)
	{
		string allFileNamesAndSizes;
		int i;
		int k = inputFileNames.size();

		allFileNamesAndSizes += to_string(k);
		allFileNamesAndSizes += '|';
		for (i = 0 ; i < k; i++) {
			allFileNamesAndSizes += inputFileNames[i];
			allFileNamesAndSizes += '|';
			allFileNamesAndSizes += to_string(inputFileSizes[i]);
			allFileNamesAndSizes += '|';
		}
		
		allFileNamesAndSizes += outputFileName;
		allFileNamesAndSizes += '|';
		allFileNamesAndSizes += to_string(outputFileSize);
		allFileNamesAndSizes += '|';

		allFileNamesAndSizes += (lastLevel == true ? '1' : '0');
		allFileNamesAndSizes += '|';

		return allFileNamesAndSizes;
	}

	int sendToCompute(vector <string> &inputFileNames, vector <size_t> inputFileSizes, string outputFileName, size_t outputFileSize, bool lastLevel)
	{
		string allFileNamesAndSizes = createString(inputFileNames, inputFileSizes, outputFileName, outputFileSize, lastLevel);
		int i;
		int k = inputFileSizes.size();
		int count;

		//char buf[BUFSIZE];

		printf("write to compute fd\n");
		int ret = write(write_fd, allFileNamesAndSizes.c_str(), allFileNamesAndSizes.size());
		if (ret < 0) {
			printf("Error writing data %s\n", strerror(errno));
			exit(1);
		}
		printf("write to compute fd complete\n");
	
		printf("read/write complete\n");

		// read output from the computational disk device
	
		printf("reading from compute\n");
		int result = read(read_fd, &count, sizeof(int));
		if (result < 0) {
			printf("%s:Could not read %s\n", __func__, strerror(errno));
			exit(1);
		}
		printf("Count returned from disk %d\n", count);
		return count;
	}

//    void addRunsCompute(vector<DiskRun<K, V> *> &runList, const unsigned long runLen, bool lastLevel) {
    int addRunsCompute(vector<string> inputFileNames, vector <size_t> inputFileSizes, string outputFileName, size_t outputFileSize , bool lastLevel) {
	
	vector <KVPair_t *> input_maps;
	KVPair_t * output_map;
	int compute_j;
 
	compute_j = sendToCompute(inputFileNames, inputFileSizes, outputFileName, outputFileSize, lastLevel);
 
	// move this to compute.
	/*
	int num_ip_files = inputFileNames.size();
	
	for (int curr = 0; curr < num_ip_files ; curr++)
	{
		input_maps.push_back(init_map(inputFileNames[curr],inputFileSizes[curr]));
	}

	output_map = init_map(outputFileName, outputFileSize);

        StaticHeap h = StaticHeap((int) inputFileNames.size(), KVINTPAIRMAX);
        vector<int> heads(inputFileNames.size(), 0);
        for (int i = 0; i < inputFileNames.size(); i++){
            KVPair_t kvp;
		memcpy(&kvp,input_maps[i], sizeof(KVPair_t));
            h.push(KVIntPair_t(kvp, i));
        }
        
        int j = -1;
        K lastKey = INT_MAX;
        unsigned lastk = INT_MIN;
        while (h.size != 0){
            auto val_run_pair = h.pop();
            assert(val_run_pair != KVINTPAIRMAX); // TODO delete asserts
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
                h.push(KVIntPair_t(kvp, k));
            }    
        }
       	 
	KVPair_t tmp;
	memcpy(&tmp, output_map + j , sizeof(KVPair_t));
        if (lastLevel && tmp.value == V_TOMBSTONE){
            --j;
        }
	// return back from compute.
	*/
	/*
	
	move these two functions to lsm.hpp as we want to not port
	fence pointer logic to compute process at the moment.
	TODO: revisit this design decision.
        runs[_activeRun]->setCapacity(j + 1);
        runs[_activeRun]->constructIndex();
	*/
	/*
	_prevRun = _activeRun;
        if(j + 1 > 0){
            ++_activeRun;
        }

	// write output data back to disk 
	exitMap(output_map, outputFileName, outputFileSize);
*/
	// input maps were read only. they are freed by call to distructor
	return compute_j;
    }
 

/*    
    void addRuns(vector<DiskRun<K, V> *> &runList, const unsigned long runLen, bool lastLevel) {
        

        StaticHeap h = StaticHeap((int) runList.size(), KVINTPAIRMAX);
        vector<int> heads(runList.size(), 0);
        for (int i = 0; i < runList.size(); i++){
            KVPair_t kvp = runList[i]->map[0];
            h.push(KVIntPair_t(kvp, i));
        }
        
        int j = -1;
        K lastKey = INT_MAX;
        unsigned lastk = INT_MIN;
        while (h.size != 0){
            auto val_run_pair = h.pop();
            assert(val_run_pair != KVINTPAIRMAX); // TODO delete asserts
            if (lastKey == val_run_pair.first.key){
                if( lastk < val_run_pair.second){
                    runs[_activeRun]->map[j] = val_run_pair.first;
                }
            }
            else {
                ++j;
                if ( j != -1 && lastLevel && runs[_activeRun]->map[j].value == V_TOMBSTONE){
                    --j;
                }
                runs[_activeRun]->map[j] = val_run_pair.first;
            }
            
            lastKey = val_run_pair.first.key;
            lastk = val_run_pair.second;
            
            unsigned k = val_run_pair.second;
            if (++heads[k] < runList[k]->getCapacity()){
                KVPair_t kvp = runList[k]->map[heads[k]];
                h.push(KVIntPair_t(kvp, k));
            }
                
        }
        
        if (lastLevel && runs[_activeRun]->map[j].value == V_TOMBSTONE){
            --j;
        }
        runs[_activeRun]->setCapacity(j + 1);
        runs[_activeRun]->constructIndex();

	_prevRun = _activeRun;
        if(j + 1 > 0){
            ++_activeRun;
        }
    }
*/   
    void addRunByArray(KVPair_t * runToAdd, const unsigned long runLen){
        assert(_activeRun < _numRuns);
        assert(runLen == _runSize);
        runs[_activeRun]->writeData(runToAdd, 0, runLen);
        runs[_activeRun]->constructIndex();
        _activeRun++;
    }
    
    
    vector<DiskRun<K,V> *> getRunsToMerge(){
        vector<DiskRun<K, V> *> toMerge;
        for (int i = 0; i < _mergeSize; i++){
            toMerge.push_back(runs[i]);
        }

        return toMerge;
        
    }
    
    void freeMergedRuns(vector<DiskRun<K,V> *> &toFree){
        assert(toFree.size() == _mergeSize);
        for (int i = 0; i < _mergeSize; i++){
            assert(toFree[i]->_level == _level);
            delete toFree[i];
        }
        runs.erase(runs.begin(), runs.begin() + _mergeSize);
        _activeRun -= _mergeSize;
        for (int i = 0; i < _activeRun; i++){

            runs[i]->_runID = i;

            string newName = ("C_" + to_string(runs[i]->_level) + "_" + to_string(runs[i]->_runID) + ".txt");
            
            if (rename(runs[i]->_filename.c_str(), newName.c_str())){
                perror(("Error renaming file " + runs[i]->_filename + " to " + newName).c_str());
                exit(EXIT_FAILURE);
            }
            runs[i]->_filename = newName;
        }
        
        for (int i = _activeRun; i < _numRuns; i++){
            DiskRun<K, V> * newRun = new DiskRun<K,V>(_runSize, _pageSize, _level, i, _bf_fp);
            runs.push_back(newRun);
        }
    }
    
    bool levelFull(){
        return (_activeRun == _numRuns);
    }
    bool levelEmpty(){
        return (_activeRun == 0);
    }
    
    V lookup (const K &key, bool &found) {
        int maxRunToSearch = levelFull() ? _numRuns - 1 : _activeRun - 1;
        for (int i = maxRunToSearch; i >= 0; --i){
            if (runs[i]->maxKey == INT_MIN || key < runs[i]->minKey || key > runs[i]->maxKey || !runs[i]->bf.mayContain(&key, sizeof(K))){
                continue;
            }
            V lookupRes = runs[i]->lookup(key, found);
            if (found) {
                return lookupRes;
            }
            
        }
        
        return (V) NULL;
        
    }
    unsigned long num_elements(){
        unsigned long total = 0;
        for (int i = 0; i < _activeRun; ++i)
            total += runs[i]->getCapacity();
        return total;
    }
};
#endif /* diskLevel_h */
