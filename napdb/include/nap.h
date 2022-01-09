#ifndef _NAP_H_
#define _NAP_H_

#include "count_min_sketch.h"
#include "nap_common.h"
#include "nap_meta.h"
#include "slice.h"
#include "timer.h"
#include "topology.h"

#include <algorithm>
#include <string>
#include <thread>
#include <vector>

namespace nap {

struct aligns(kCachelineSize) ThreadMeta {
	uint64_t epoch;
	uint64_t op_seq;
	uint64_t hit_in_cap;
	bool is_in_nap;

	ThreadMeta():epoch(0), op_seq(0), hit_in_cap(0), is_in_nap(false){}
};

extern ThreadMeta thread_meta_array[kMacThreadCnt];

enum UndoLogType{
	Invalid,
	TYPE_1,
	TYPE_2
};

struct aligns(64) UndoLog {
	UndoLogType type;
	void *cur;
	void *pre;
	void *gc;

	UndoLog() { type=UndoLogType::Invalid; }

	void logging_type1(void *_cur, void *_pre){
		cur = _cur;
		pre = _pre;
		compiler_barrier();
		type = UndoLogType::TYPE_1;
		//pflush
	}

	void logging_type2(void *_gc, void *_pre){
		gc = _gc;
		pre = _pre;
		compilter_barier();
		type = UndoLogType::TYPE_2;
		//pflush
	}

	void truncate(){
		type = UndoLogType::Invalid;
		//pflush
	}
};

template <class T> class Nap {
private:
	T *raw_index;

	CountMin *CM;
	int hot_cnt;

	int kSampleInterval{1};
	double kSwitchInterval{5.0};

	// In PM
	NapMeta *g_cur_meta;
	NapMeta *g_pre_meta;
	NapMeta *g_fc_meta;

	UndoLog *undo_log;

	std::atomic<uint64_t> g_cur_epoch;
	std::atomic<uint64_t> epoch_seq_lock;
	ReadFriendlyLock data_race_lock;
	ReadFriendlyLock shift_global_lock;

	void nap_shift();

	bool find_in_views(GVView::Entry *e, NapMeta *pre_meta, const Slice &key, std::string &value);

	void persist_meta_ptrs() { //pflush(&g_cur_meta); }

	std::thread shift_thread;
	std::atomic_bool shift_thread_is_ready;

public:
	Nap(T *raw_index, int hot_cnt = kHotKeys);
	~Nap();

	void put
};

} // namespace nap

#endif // _NAP_H_
