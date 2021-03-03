#include <iostream>
#include <fstream>
#include <pthread.h>
#include <windows.h>
#include <types.h>

int count = 0;

void* thr_func(void* arg){
    std::fstream outfile;
    outfile.open("outputfile",std::ios::out);
    int time=0;
    timespec start;
    timespec end;
    int useconds;
    clock_gettime(CLOCK_MONOTONIC, &start);
    while(time < 5){
        clock_gettime(CLOCK_MONOTONIC, &end);
        int useconds = (int) (((end.tv_sec - start.tv_sec) * 1000000) +
				   ((end.tv_nsec - start.tv_nsec) / 1000));
        outfile<<time<<","<<count<<","<<useconds<<std::endl;
        start = end;
        time++;
        Sleep(500);
        count = 0;
        std::cout<<"Hello\n";
    }
    return (void*)0;
}

int main()
{
    pthread_t tid;
    pthread_create(&tid,NULL,&thr_func,NULL);
    while(true){
        count++;
    }
}