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

	ThreadMeta(): epoch(0), op_seq(0), hit_in_cap(0), is_in_nap(false){}
};

extern ThreadMeta thread_meta_array[kMaxThreadCnt];

enum UndoLogType{
	Invalid,
	TYPE_1,
	TYPE_2,
};

struct aligns(64) UndoLog {
	UndoLogType type;
	void *cur;
	void *pre;
	void *gc;

	UndoLog() { type = UndoLogType::Invalid; }

	void logging_type1(void *_cur, void *_pre){
		cur = _cur;
		pre = _pre;
		compiler_barrier();
		type = UndoLogType::TYPE_1;
		//persistent::clflush(this)
	}

	void logging_type2(void *_gc, void *_pre){
		gc = _gc;
		pre = _pre;
		compilter_barier();
		type = UndoLogType::TYPE_2;
		//persistent::clflush(this)
	}

	void truncate(){
		type = UndoLogType::Invalid;
		//persistent::clflush(this)
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
	NapMeta *g_gc_meta;

	UndoLog *undo_log;

	std::atomic<uint64_t> g_cur_epoch;
	std::atomic<uint64_t> epoch_seq_lock;
	ReadFriendlyLock data_race_lock;
	ReadFriendlyLock shift_global_lock;

	void nap_shift();

	bool find_in_views(GVView::Entry *e, NapMeta *pre_meta, const Slice &key, std::string &value);

	void persist_meta_ptrs() { /* persistent::clflush(&g_cur_meta); */ }

	std::thread shift_thread;
	std::atomic_bool shift_thread_is_ready;

public:
	Nap(T *raw_index, int hot_cnt = kHotKeys);
	~Nap();

	bool Get(const Slice &key, std::string &value);
	void Put(const Slice &key, const Slice &value);

	//void Del(const Slice &key);
	
	void set_sampling_interval(int v){
		kSampleInterval = v;
		mfence();
	}

	void set_switch_interval(double v){
		kSwitchInterval = v;
		mfence();
	}

	void clear(){
		for(int i=0;i<kMaxThreadCnt;i++){
			thread_meta_array[i].op_seq = 0;
			thread_meta_array[i].hit_in_cap = 0;
		}
	}
	
	void show_statistics() {
		uint64_t all_op = 0;
		uint64_t all_hit = 0;
		for (int i = 0; i < kMaxThreadCnt; ++i) {
		        all_op += thread_meta_array[i].op_seq;
		        all_hit += thread_meta_array[i].hit_in_cap;
		}
		printf("nap hit ratio: %f\n", all_hit * 1.0 / all_op);
	}
};

template <class T>
Nap<T>::Nap(T *raw_index, int hot_cnt):raw_index(raw_index), hot_cnt(hot_cnt), shift_thread_is_ready(false){
	//for shift thread
	
	undo_log = static_cast<UndoLog*>malloc(sizeof(UndoLog));
		
	shift_thread = std::thread(&Nap<T>::nap_shift, this);
	while(!shift_thread_is_ready);
}

template <class T> Nap<T>::~Nap(){
	shift_thread_is_ready.store(false);
	shift_thread.join();
}

template <class T>
void Nap<T>::Put(const Slice &key, const Slice &value){

#ifdef USE_GLOBAL_LOCK
	shift_global_lock.read_lock();
#endif

	auto &thread_meta = thread_meta_array[Topology::threadID()];

	NapMeta *cur_meta, *pre_meta;
	uint64_t version, next_version, cur_epoch;
	thread_meta.is_in_nap = true;
	thread_meta.op_seq++;

	//sampling and publish access pattern
	if(thread_meta.op_seq%kSampleInterval==0){
		CM->record(key);
	}

retry:
	version = epoch_seq_lock.load(std::memory_order_acquire);
	if(version%2!=0){
		goto retry;
	}
	mfence();

	/*save a metadata */
	cur_meta = g_cur_meta;
	pre_meta = g_pre_meta;
	cur_epoch = g_cur_epoch;

	compiler_barrier();
	next_version = epoch_seq_lock.load(std::memory_order_acquire);
	if (next_version != version) {
		goto retry;
	}

	thread_meta.epoch = cur_epoch;
	
	assert(cur_meta);
	GVView::Entry *e;
	if(cur_meta->gv_view->get_entry(key, e)) { //in cur_meta
		thread_meta.hit_in_cap++;
		bool is_writer = false;
		
re_lock:
		if(!e->l.try_putlock(is_writer)){
			if(is_writer){
				// two requests of the same key, one of which can be returned directly after waiting for unlock.
#ifdef USE_GLOBAL_LOCK
				shift_global_lock.read_unlock();
#endif
				while(!e->l.is_unlock()) ;
				return;
			}
			goto re_lock;
		}

		if(e->shifting) {
			// I am previous view, cannot update now
			e->l.putUnlock();
			goto retry;
		}

		if(e->location==WhereIsData::IN_PREVIOUS_EPOCH) {
			GVView::Entry *pre_e;
			if(pre_meta->gv_view->get_entry(key, pre_e)) {
				pre_e->l.wLock();
				pre_e->shifting = true;
				pre_e->l.wUnlock();
			} else {
				asset(false);
			}
		}

		e->next_version();

		e->v = value.ToString();
		e->is_deleted = false;

		if(e->location != WhereIsData::IN_CURRENT_EPOCH){
			e->location = WhereIsData::IN_CURRENT_EPOCH;
		}

		e->l.putUnlock();
	}else if(pre_meta) {
		if(pre_meta->gv_view->get_entry(key, e)) { // in pre_meta
			thread_meta.is_in_nap = false;
			while(g_pre_meta!=nullptr) {
				mfence();
			}
			thread_meta.is_in_nap = true;
			goto retry;
		} else {
			raw_index->put(key, value);
		}
	} else {
		data_race_lock.read_lock();
		if(pre_meta==nullptr){
			raw_index->put(key, value);
		}else{ // is_shifting
			assert(false);
			data_race_lock.read_unlock();
			goto retry;
		}
		data_race_lock.read_unlock();
	}

	compiler_barrier();
	thread_meta.is_in_nap = false;

#ifdef USE_GLOBAL_LOCK
	shift_global_lock.read_unlock();
#endif
}

template <class T>
bool Nap<T>::Get(const Slice &key, std::string &value){

#ifdef USE_GLOBAL_LOCK
	shift_global_lock.read_lock();
#endif

	auto &thread_meta = thread_meta_array[Topology::threadID()];
	thread_meta.is_in_nap = true;
	thread_meta.op_seq++;

	// sampling and publish access pattern
	if(thread_meta.op_seq % kSampleInterval == 0) {
		CM->record(key);
	}

	NapMeta *cur_meta, *pre_meta;
	uint64_t version, next_version, cur_epoch;
	next_version = 0;

	int count = 0;
retry:
	if(++count > 10000) {
		printf("exit with retry! %ld %ld\n", version, next_version);
		exit(-1);
	}
	version = epoch_seq_lock.load(std::memory_order::memory_order_acquire);
	if(version % 2 != 0){
		goto retry;
	}

	mfence();
	cur_meta = g_cur_meta;
	pre_meta = g_pre_meta;
	cur_epoch = g_cur_epoch;

	compiler_barrier();
	next_version = epoch_seq_lock.load(std::memory_order::memory_acquire);
	if(next_version != version) {
		goto retry;
	}

	thread_meta.epoch = cur_epoch;

	bool res = true;
	assert(cur_meta);
	GVView::Entry *e;
	if(cur_meta->gv_view->get_entry(key, e)) { //in cur_meta
		thread_meta.hit_in_cap++;
		res = find_in_views(e, pre_meta, key, value);
	}else if(pre_meta){
		assert(pre_meta->gv_view);
		if(pre_meta->gv_view->get_entry(key, e)){ //in pre_meta
			res = find_in_views(e, nullptr, key, value);
		}else{
			res = raw_index->get(key, value); //in raw index
		}
	}else{
		res = raw_index->get(key, value); //in raw index
	}

	compiler_barrier();
	thread_meta.is_in_nap = false;

#ifdef USE_GLOBAL_LOCK
	shift_global_lock.read_unlock();
#endif
	
	return res;
}

template <class T>
bool Nap<T>::find_in_views(GVView::Entry *e, NapMeta *pre_meta, const Slice &key, std::string &value){

retry:
	bool res = false;
	e->l.rLock();

	switch(e->location){

	case WhereIsData::IN_CURRENT_EPOCH:

		if(!e->is_deleted){
			value = e->v;
		}
		e->l.rUnlock();
		res = !e->is_deleted;
		break;

	case WhereIsData::IN_PREVIOUS_EPOCH:

		assert(pre_meta);
		GVView::Entry *pre_e;
		e->l.rUnlock();
		e->l.wLock();

		if(e->location!=WhereIsData::IN_PREVIOUS_EPOCH){
			e->l.wUnlock();
			goto retry;
		}

		if (pre_meta->gv_view->get_entry(key, pre_e)) {
			pre_e->l.wLock();
			pre_e->shifting = true;
			e->location = WhereIsData::IN_CURRENT_EPOCH;
			if (pre_e->location==WhereIsData::IN_CURRENT_EPOCH) {
				e->is_deleted = pre_e->is_deleted;
				e->v = pre_e->v;
				value = e->v;
				res = !e->is_deleted;
			} else if (pre_e->location == WhereIsData::IN_RAW_INDEX) {
				res = raw_index->get(key, value);
				if (res) {
					e->v = value;
					pre_e->v = value;
				} else {
					e->is_deleted = true;
					pre_e->is_deleted = true;
				}
			} else {
				printf("%ld\n", *(uint64_t *)key.ToString().c_str());
				assert(false);
			}
			pre_e->l.wUnlock();
		} else {
			printf("%ld\n", *(uint64_t *)key.ToString().c_str());
			assert(false);
		}
		e->l.wUnlock();
		break;

	case WhereIsDara::IN_RAW_INDEX:

		e->l.rUnlock();
		e->l.wLock();
		if (e->location != WhereIsData::IN_RAW_INDEX) {
			e->l.wUnlock();
			goto retry;
		}

		e->location = WhereIsData::IN_CURRENT_EPOCH;
		res = raw_index->get(key, value);

		if (res) {
			e->v = value;
		} else {
			e->is_deleted = true;
		}
		e->l.wUnlock();
		break;
	default:
		break;
	}

	return res;
}

template <class T>
void Nap<T>::nap_shift() {
	static auto sort_func = [](const NapPair &a, const NapPair &b){
		return a.first < b.first;
	};

	bindCore(Topology::threadID());

	CM = new CountMin(hot_cnt);

	g_cur_epoch = 1;
	epoch_seq_lock = 0;
	g_cur_meta = g_pre_meta = g_gc_meta = nullptr;

	std::vector<NapPair> cur_list;
	g_cur_meta = new NapMeta(cur_list);

	shift_thread_is_ready.store(true);

	printf("shift thread finished init [%d]. \n", Topology::threadID());

	const int kPreHotest = 8;
	std::string pre_hotest_keys[kPreHotest];
	while (shift_thread_is_ready) {

		CM->reset(); //clear count-min sketch and min heap
		CM->poll_workloads(kSwitchInterval /* seconds */);

		auto &l = CM->get_list();

		std::sort(l.begin()+1, l.end(), [](const nap::Node &a, const nap::Node &b){ return a.cnt > b.cnt; });
		if(l.size() <= kPreHotest || l[1].cnt < 100) { // not need shift
			continue;
		}

		if(l[1].cnt / l.back().cnt < 3) { // uniform workload
			continue;
		}

		bool need_shift = true;
		for(int i=0; i<kPreHotest; ++i) {
			if(l[1].key == pre_hotest_keys[i]){
				need_shift = false;
				break;
			}
		}

		if(!need_shift) {
			continue;
		}

#ifdef USE_GLOBAL_LOCK
		shift_global_lock.write_lock();
#endif

		for(size_t i=0; i<kPreHotest; ++i){
			pre_hotest_keys[i] = l[1+i].key;
		}

		std::vector<NapPair> new_list;
		for(uint64_t k=1;k<l.size(); ++k){
			new_list.push_back({l[k].key, WhereIsData::IN_RAW_INDEX});
		}

		std::sort(new_list.begin(), new_list.end, sort_func);

		uint64_t overlapped_cnt = 0;
		for(size_t i=0, j=0; i<cur_list.size()&&j<new_list.size();){
			int cmp = cur_list[i].first.compare(new_list[j].first);
			if(cmp==0) { // overlapped kv in different epoch
				new_list[j].second = WhereIsData::IN_PREVIOUS_EPOCH;
				i++, j++;
				overlapped_cnt++;
			} else if (cmp<0) {
				i++;
			} else {
				j++;
			}
		}

		if(overlapped_cnt>0.75*hot_cnt) { // not need shift
			continue;
		}

		auto new_meta = new NapMeta(new_list);
		auto old_meta = g_cur_meta;

		cur_list.swap(new_list);

		undo_log->logging_type1(g_cur_meta, g_pre_meta); // undo logging
		data_race_lock.write_lock();
		epoch_seq_lock.fetch_add(1);
		g_cur_meta = new_meta;
		// sleep(1);
		g_pre_meta = old_meta;
		// sleep(1);
		g_cur_epoch++;
		epoch_seq_lock.fetch_add(1);
		data_race_lock.write_unlock();

		// persist_meta_ptrs();
		undo_log->truncate();

		// wait until all threads learn that a shifting is ongoing
		for(int i=0; i<kMacThreadCnt; ++i){
			auto &m = thread_meta_array[i];
			while(m.epoch != g_cur_epoch &&m.is_in_nap) {
				mfence();
			}
		}

		old_meta->fluch_gv_view<T>(raw_index); // flush the NAL into raw index
		new_meta->relocate_value(old_meta); //finish lazy initialization

		compiler_barrier();

		auto del_meta = g_pre_meta;

		undo_log->logging_type2(g_gc_meta, g_pre_meta);
		epoch_seq_lock.fetch_add(1);
		g_gc_meta = g_pre_meta;
		g_pre_meta = nullptr;
		epoch_seq_lock.fetch_add(1);

		// persistent_meta_ptrs();
		undo_log->truncate();

		// wait for a grace period for safe deallocation
		for (int i=0; i<kMaxThreadCnt; ++i) {
			auto &m = thread_meta_array[i];
			auto cur_seq = m.op_seq;

		retry:
			if (!m.is_in_nap) {
				continue;
			}
			if (m.op_seq != cur_seq) {
				continue;
			}

			mfence();
			goto retry;
		}

		delete del_meta;
		g_gc_meta = nullptr;
		// persist_meta_ptrs();

#ifdef USE_GLOBAL_LOCK
		shift_global_lock.write_unlock();
#endif
	}
	printf("shift thread stopped.\n");
}

} // namespace nap

#endif // _NAP_H_
