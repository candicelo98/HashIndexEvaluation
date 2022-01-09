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

#include "napcommon.h"

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
