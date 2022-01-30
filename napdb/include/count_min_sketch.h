#ifndef _COUNT_MIN_SKETCH_H_
#define _COUNT_MIN_SKETCH_H_

#include "hash32.h"
#include "murmur_hash2.h"
#include "top_k.h"
#include "slice.h"
#include "nap_common.h"
#include "topology.h"
#include "timer.h"

#include <functional>
#include <queue>
#include <set>
#include <string>
#include <unordered_set>

namespace nap {

struct __attribute__((__packed__)) PerRecord {
	uint64_t timestamp;
	char *v;

	PerRecord() : timestamp(0), v(nullptr) { }
};

struct RecordCursor {
	uint64_t last_ts;
	uint32_t last_index;

	RecordCursor() : last_ts(0), last_index(0) { }
}

static_assert(sizeof(PerRecord)==16, "Size of PerRecord should be 16");

class CountMin {
private:
	int hot_keys_cnt; //hot set size

	const static int kHashCnt = 3;
	const static int kBloomLength = 876199;
	uint32_t *bloomArray[kHashCnt];
	uint64_t hashSeed[32] = {931901, 1974701, 7296907};

	TopK topK;

	const uint32_t kRecordBufferSize = 20000;
	PerRecord *recordBuffer[kMaxThreadCnt];
	RecordCursor cursors[kMaxThreadCnt];

public:
	CountMin(int hot_keys_cnt):hot_keys_cnt(hot_keys_cnt), topK(hot_keys_cnt){
		//initialize bloom arrays
		for(int i=0;i<kHashCnt;i++){
			bloomArray[i] = new uint32_t[kBloomLength];
			memset(bloomArray[i], 0, kBloomLength*sizeof(uint32_t));
		}

		//initialize per thread sampling buffer
		for(int i=0;i<kMaxThreadCnt;i++){
			recordBuffer[i] = new PerRecord[kRecordBufferSize];
		}
	}

	~CountMin(){
		for(int i=0;i<kHashCnt;i++){
			if(bloomArray[i]){
				delete[] bloomArray[i];
			}
		}

		for(int i=0;i<kMaxThreadCnt;i++){
			if(recordBuffer[i]){
				delete[] recordBuffer[i];
			}
		}
	}

	std::vector<Node>& get_list() { return topK.getList(); }

	void reset(){
		topK.reset();
		for(int i=0;i<kHashCnt;i++){
			memset(bloomArray[i], 0, kBloomLength*sizeof(uint32_t));
		}
	}


	void record(const Slice &key){
		// for threads that access keys
		static thread_local int index = 0;
		static thread_local char *free_buffer = nullptr;
		static thread_local PerRecord *thread_records = recordBuffer[Topology::threadID()];

		char *buf;
		if(free_buffer && *(uint32_t*)(free_buffer)>=key.Size()){
			buf = free_buffer;
			free_buffer = nullptr;
		}else{
			buf = (char*)malloc(key.size()+sizeof(uint32_t));
		}

		*(uint32_t)buf = key.size();
		memcpy(buf+sizeof(uint32_t), key.data(), key.size());

		char *old_ptr = thread_records[index].v;
		
		thread_records[index].v = buf;
		thread_records[index].timestamp = asm_rdtsc();

		if(!old_ptr) {
			if(!free_buffer){
				free_buffer = old_ptr;
			}else if (*(uint32_t*)(free_buffer)<*(uint32_t*)(old_ptr)){
				free(free_buffer);
				free_buffer = old_ptr;
			}
		}

		index = (index+1) % kRecordBufferSize;
	}

	void poll_workloads(double seconds){
		uint64_t ns = seconds * (1000ull*1000*1000);
		uint16_t kBatchPerThread = 8;

		Timer timer;
		timer.begin();

		while(true){
			for(int i=0;i<kMaxThreadCnt;i++){
				for(int j=0;j<kBatchPerThread;j++){
					RecordCursor &c = cursors[i];
					RecordCursor &r = record_buffer[i][c.last_index];
					if(r.timestamp<c.last_ts || r.v==nullptr){
						break; // invalid record
					}

					this->access_a_key(Slice(r.v+sizeof(uint32_t), *(uint32_t*)r.v));
					c.last_ts = r.timestamp;
					c.last_index = (c.last_index+1)%kRecordBufferSize;
				}
				if(timer.end()>ns){
					return;
				}
			}
		}
	}

	void access_a_key(const Slice &key){
		uint64_t hash_val[kHashCnt];
		for(int i=0;i<kHashCnt;i++){
			hash_val[i] = (MurmurHash64A(key.data(), key.size(), hashSeed[i]))%kBloomLength;
		}

		uint32_t minFreq = ++bloomArray[0][hash_val[0]];
		for(int i=1;i<kHashCnt;i++){
			uint32_t tmp = ++bloomArray[i][hash_val[i]];
			if(tmp < minFreq){
				minFreq = tmp;
			}
		}
		topK.access_a_key(key.ToString(). minFreq);
	}
};

} // namespace nap

#endif //_COUNT_MIN_SKETCH_H_
