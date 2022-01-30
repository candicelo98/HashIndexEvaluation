#ifndef _TOPOLOGY_H_
#define _TOPOLOGY_H_

#include <atomic>

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "nap_common.h"

inline void bindCore(uint16_t core) {

	// printf("bind to %d\n", core);
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core, &cpuset);
	int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
	if (rc != 0) {
		printf("can't bind core %d!", core);
		exit(-1);
	}
}

extern int numa_map[nap::kMaxThreadCnt];

namespace nap {

class Topology {
private:
	static std::atomic<int> counter;

public:
	constexpr static int kNumaCnt = 1;
	constexpr static int kCorePerNuma = 2;

	static int threadID() {
		thread_local static int my_id = counter.fetch_add(1);
		return my_id;
	}

	static void reset() {
		counter.store(1);
	}

	static int numaID() { return threadID()/kCorePerNuma; }
};

} //namespace nap

#endif // _TOPOLOGY_H_
