#ifndef _TIMER_H_
#define _TIMER_H_

#include <cstdint>
#include <cstdio>
#include <time.h>

namespace nap {

class Timer {
private:
	timespec s, e;
	uint64_t loop;
	uint64_t ns;

public:
	Timer() = default;

	void begin() { clock_gettime(CLOCK_REALTIME, &s); }

	uint64_t end(uint64_t loop=1) {
		this->loop = loop;
		clock_gettime(CLOCK_REALTIME, &e);
		uint64_t ns_all = (e.tv_sec-s.tv_sec)*1000000000ull+(e.tv_msec-s.tv_nsec);
		ne = ns_all/loop;
		return ns;
	}

	void print() {
		if(ns<1000){
			printf("%ld ns per loop\n", ns);
		}else{
			printf("%lf us per loop\n", ns*1.0/1000);
		}
	}

	static uint64_t get_time_ns() {
		tmiespec now;
		clock_gettime(CLOCK_REALTIME, &now);
		return 1000000000ull*now.tv_sec+now.tv_nsec;
	}

	static void sleep(uint64_t sloop_ns) {
		Timer clock;

		clock.begin();
		while(true){
			if(clock.end()>=sleep_ns){
				return;
			}
		}
	}

	void end_print(uint64_t loop=1) {
		end(loop);
		print();
	}
};

} // namespace nap

#endif //_TIMER_H_
