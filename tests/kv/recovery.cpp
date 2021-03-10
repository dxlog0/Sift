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
#include <fstream>
#include <time.h>

#include <random>
//op
#define ST_OP_PUT 0
#define ST_OP_GET 1

//LATENCY
#define MAX_LATENCY 1000 //in us
#define LATENCY_BUCKETS 1000
#define LATENCY_PRECISION (MAX_LATENCY / LATENCY_BUCKETS) //latency granularity in us

// DEBUG
#define ENABLE_ASSERTIONS 0

// 10ms throughput
int completedOps = 0; //count the number of ops that completed in 10ms
bool allComplete = false;

void* countThroughput(void * no_arg){
    std::fstream infile;
    infile.open("./results/revovery.csv");
    infile<<"Timespan/ms,completedOps"<<endl;
    completedOps = 0;
    long times = 0;
    struct timespec start, end;
    clock_gettime(CLOCK_REALTIME, &start);
    while (!allComplete)
    {
        usleep(10000);//usleep 10000us = 10ms 
        clock_gettime(CLOCK_REALTIME, &end);
        double seconds = (end.tv_sec - start.tv_sec) + (double) (end.tv_nsec - start.tv_nsec) / 1000000001;
        start = end;
        infile<<times<<","<<completedOps<<","<<seconds<<std::endl;
        times++;
        completedOps = 0;
    }
    infile.close();
}

const int num_keys = KV_SIZE;

int getOp() {
    static thread_local std::default_random_engine generator;
    std::uniform_int_distribution<int> intDistribution(0,99);
    return intDistribution(generator);
}

/* ---------------------------------------------------------------------------
----------------------------------- LATENCY -------------------------------
---------------------------------------------------------------------------*/
struct latency_counters{
    uint32_t read_reqs[LATENCY_BUCKETS + 1];
    uint32_t write_reqs[LATENCY_BUCKETS + 1];
    int max_read_latency;
    int max_write_latency;
    long long total_measurements;
};
struct latency_counters latency_count;

//Add latency to histogram (in microseconds)
static inline void
bookkeep_latency(int useconds, uint8_t op)
{
	uint32_t* latency_array;
	int* max_latency_ptr;
	switch (op){
		case ST_OP_PUT:
			latency_array = latency_count.write_reqs;
			max_latency_ptr = &latency_count.max_write_latency;
			break;
		case ST_OP_GET:
			latency_array = latency_count.read_reqs;
			max_latency_ptr = &latency_count.max_read_latency;
			break;
		default: assert(0);
	}
	latency_count.total_measurements++;
	if (useconds > MAX_LATENCY)
		latency_array[LATENCY_BUCKETS]++;
	else
		latency_array[useconds / LATENCY_PRECISION]++;

	if(*max_latency_ptr < useconds)
		*max_latency_ptr = useconds;
}

// Necessary bookkeeping to initiate the latency measurement
static inline void
start_latency_measurement(struct timespec *start)
{
	clock_gettime(CLOCK_MONOTONIC, start);
}

static inline void
stop_latency_measurement(uint8_t req_opcode, struct timespec *start)
{
	struct timespec end;
	clock_gettime(CLOCK_MONOTONIC, &end);
	int useconds = (int) (((end.tv_sec - start->tv_sec) * 1000000) +
				   ((end.tv_nsec - start->tv_nsec) / 1000));
	if (ENABLE_ASSERTIONS) assert(useconds >= 0);
//	printf("Latency of %s %u us\n", code_to_str(req_opcode), useconds);
	bookkeep_latency(useconds, req_opcode);
}

//assuming microsecond latency
void dump_latency_stats(int op_num, int read_ratio)
{
    FILE *latency_stats_fd;
    char filename[128];
    const char* path = "./results/latency";

    sprintf(filename, "%s/%s_latency_op_%d_read_%d.csv", path,
            "Sift",
            op_num, read_ratio);

    latency_stats_fd = fopen(filename, "w");
    fprintf(latency_stats_fd, "#---------------- Read Reqs --------------\n");
    for(int i = 0; i < LATENCY_BUCKETS; ++i)
        fprintf(latency_stats_fd, "reads: %d, %d\n",i * LATENCY_PRECISION, latency_count.read_reqs[i]);
    fprintf(latency_stats_fd, "reads: -1, %d\n", latency_count.read_reqs[LATENCY_BUCKETS]); //print outliers
    fprintf(latency_stats_fd, "reads-hl: %d\n", latency_count.max_read_latency); //print max read latency

    fprintf(latency_stats_fd, "#---------------- Write Reqs ---------------\n");
    for(int i = 0; i < LATENCY_BUCKETS; ++i)
        fprintf(latency_stats_fd, "writes: %d, %d\n",i * LATENCY_PRECISION, latency_count.write_reqs[i]);
    fprintf(latency_stats_fd, "writes: -1, %d\n", latency_count.write_reqs[LATENCY_BUCKETS]); //print outliers
    fprintf(latency_stats_fd, "writes-hl: %d\n", latency_count.max_write_latency); //print max write latency

    fclose(latency_stats_fd);

    printf("Latency stats saved at %s\n", filename);
}

int main(int argc, char **argv) {
    if (argc < 5) {
        printf("Usage: %s server_addr server_port numOps readProb\n", argv[0]);
        return -1;
    }

    printConfig();

    std::string server_addr(argv[1]);
    int server_port = std::stoi(argv[2]);
    int num_ops = std::stoi(argv[3]);
    int read_prob = std::stoi(argv[4]);

    LogInfo("Starting test");
    RemoteClient client(server_addr, server_port);

    LogInfo("Populating store with " << num_keys << " values...");
    // Populate the store with values
    // Population is single-threaded, might take a long time if KV_SIZE is large
    for (int i = 0; i < num_keys; i++) {
        std::string key("keykeykey" + std::to_string(i));
        std::string value("this is a test value " + std::to_string(i));
        client.put(key, value);
    }
    LogInfo("Done populating kv store");

    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(1,num_keys-1);

    LogInfo("Running workload...");
    uint64_t completed_gets = 0;
    uint64_t completed_puts = 0;
    struct timespec start_time;

    pthread_t pthread_throughput;
    pthread_create(&pthread_throughput, NULL, countThroughput, NULL);

    for (int i = 0; i < num_ops; i++) {
        int op = getOp();
        std::string key("keykeykey" + std::to_string(dist(rng)));
        start_latency_measurement(&start_time);
        if (op < read_prob) {
            client.get(key);
            completed_gets++;
            completedOps++;
        } else {
            std::string value("this is a test string " + std::to_string(i));
            client.put(key, value);
            completed_puts++;
            completedOps++;
        }
        stop_latency_measurement(op < read_prob?1:0, &start_time);
    }
    allComplete = true;
    LogInfo("Result: " << completed_gets << " gets, " << completed_puts << " puts");
    dump_latency_stats(num_ops, read_prob);
    std::this_thread::sleep_for(std::chrono::seconds(1));

    return 0;
}