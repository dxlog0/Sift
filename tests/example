// Copyright 2019 Mikhail Kazhamiaka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "include/kv_store/remote_server.h"
#include "include/client/remote_client.h"
#include "common/common.h"

#include <random>
#include <math.h>
#include <fstream>
#include <iostream>

#define PRINT_STATS_EVERY_MSECS 10
#define NUM_CLIENT 8

const int num_keys = KV_SIZE;
volatile double w_stats = 0;

volatile uint64_t completed_gets = 0;
volatile uint64_t completed_puts = 0;

int num_ops = 0;
int read_prob = 0;

std::string timestamps() {
    std::chrono::system_clock::time_point tp = std::chrono::system_clock::now();
    std::time_t tt = std::chrono::system_clock::to_time_t(tp);
    std::tm gmt{};
    gmtime_r(&tt, &gmt);
    std::chrono::duration<double> fractional_seconds =
            (tp - std::chrono::system_clock::from_time_t(tt)) + std::chrono::seconds(gmt.tm_sec);
    std::string buffer("hr:mn:sc.xxxxxx");
    sprintf(&buffer.front(), "%02d:%02d:%08.6f", gmt.tm_hour, gmt.tm_min, fractional_seconds.count());
    return buffer;
}

int count_timespan(std::string str1, std::string str2){
    int h1=str1.find(':') , m1=str1.rfind(':'), s1=str1.find('.');
    int h2=str2.find(':') , m2=str2.rfind(':'), s2=str2.find('.');
    std::string hr1=str1.substr(0, h1), hr2=str2.substr(0, h2);
    std::string min1=str1.substr(h1+1, m1-h1-1), min2=str2.substr(h2+1, m2-h2-1); 
    std::string sec1=str1.substr(m1+1, s1-m1-1), sec2=str2.substr(m2+1, s2-m2-1);
    std::string usec1=str1.substr(s1+1, 3), usec2=str2.substr(s2+1, 3); 
    int hr3=atoi(hr2.c_str())-atoi(hr1.c_str());
    int min3=atoi(min2.c_str())-atoi(min1.c_str());
    int sec3=atoi(sec2.c_str())-atoi(sec1.c_str());
    int usec3=atoi(usec2.c_str())-atoi(usec1.c_str());
    int time3=hr3*60*60*1000+min3*60*1000+sec3*1000+usec3;
    return time3;
}

int getOp() {
    static thread_local std::default_random_engine generator;
    std::uniform_int_distribution<int> intDistribution(0,99);
    return intDistribution(generator);
}

void * print_stats_thread(void * no_arg)
{
    std::ofstream p;
    p.open("output.csv"); 
    p << "Time(ms)" << "," << "Throughout(MReqs/s)" << std::endl;
    int print_count = 0;
    double total_throughput=0;
    double curr_w_stats, pre_w_stats = w_stats;
    struct timespec start, end;
    clock_gettime(CLOCK_REALTIME, &start);
    while(true){
        usleep(PRINT_STATS_EVERY_MSECS * 1000);
        clock_gettime(CLOCK_REALTIME, &end);
        double seconds = (end.tv_sec - start.tv_sec) + (double) (end.tv_nsec - start.tv_nsec) / 1000000001;
        start = end;
        curr_w_stats = w_stats;
        total_throughput = (curr_w_stats - pre_w_stats) / seconds;
        pre_w_stats = curr_w_stats;
        print_count++;
        p << print_count * PRINT_STATS_EVERY_MSECS << "," << total_throughput << std::endl;
        printf("PRINT %d time elapsed %.4f ms, NODE Reqs/s: %.2f \n", print_count, seconds*1000, total_throughput);
    }
    p.close();
}

void * exec_populate(void * arg)
{
    RemoteClient * client = (RemoteClient *)arg;
    for (int i = 0; i < num_keys; i++) {
        std::string key("keykeykey" + std::to_string(i));
        std::string value("this is a test value " + std::to_string(i));
        client->put(key, value);
    }
    return NULL;
}


void * exec_ops(void * arg)
{
    RemoteClient * client = (RemoteClient *)arg;
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(1,num_keys-1);
    for (int i = 0; i < num_ops; i++) {
        int op = getOp();
        std::string key("keykeykey" + std::to_string(dist(rng)));
        if (op < read_prob) {
            client->get(key);
            completed_gets++;
            w_stats++;
        } 
        else {
            std::string value("this is a test string " + std::to_string(i));
            client->put(key, value);
            completed_puts++;
            w_stats++;
        }
    }
    return NULL;
}


int main(int argc, char **argv) {
    if (argc < 5) {
        printf("Usage: %s server_addr server_port numOps readProb\n", argv[0]);
        return -1;
    }

    printConfig();

    std::string server_addr(argv[1]);
    int server_port = std::stoi(argv[2]);
    num_ops = std::stoi(argv[3]);
    read_prob = std::stoi(argv[4]);

    RemoteClient * client[NUM_CLIENT];

    for(int j=0; j<NUM_CLIENT; j++){
        client[j] = new RemoteClient(server_addr, server_port);
    }

    LogInfo("Starting test");
    //RemoteClient client(server_addr, server_port);


    LogInfo("Populating store with " << num_keys << " values...");
    // Populate the store with values
    // Population is single-threaded, might take a long time if KV_SIZE is large
    
    pthread_t populate_thread[NUM_CLIENT];
    for(int j=0; j<NUM_CLIENT; j++){
        pthread_create(&populate_thread[j], NULL, exec_populate, client[j]);
    }
    for(int j=0; j<NUM_CLIENT; j++){
        pthread_join(populate_thread[j], NULL);
    }
    
    LogInfo("Done populating kv store");

    LogInfo("Running workload...");
    
    //
    pthread_t print_thread;
    pthread_create(&print_thread, NULL, print_stats_thread, NULL);

    std::string str1 = timestamps();

    //thread
    pthread_t client_thread[NUM_CLIENT];
    for(int j=0; j<NUM_CLIENT; j++){
        pthread_create(&client_thread[j], NULL, exec_ops , client[j] );
    }
    
    for(int j=0; j<NUM_CLIENT; j++){
        pthread_join(client_thread[j], NULL);
    }

    /*
    for (int i = 0; i < num_ops; i++) {
        int op = getOp();
        std::string key("keykeykey" + std::to_string(dist(rng)));

        if (op < read_prob) {
            client.get(key);
            completed_gets++;
            w_stats++;
        } else {
            std::string value("this is a test string " + std::to_string(i));
            client.put(key, value);
            completed_puts++;
            w_stats++;
        }
    }*/

    std::string str2 = timestamps();

    int time3 = count_timespan(str1, str2); 

    LogInfo("Results: " << completed_gets << " gets, " << completed_puts << " puts, Total consume " << time3 << "ms");
    LogInfo("average throughout: " << (completed_gets+completed_puts)*1000/time3 << " Reqs/s");
    //std::this_thread::sleep_for(std::chrono::seconds(1));

    
    return 0;
}


