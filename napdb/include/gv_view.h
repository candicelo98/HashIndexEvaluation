#ifndef _GV_VIEW_H_
#define _GV_VIEW_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "nap_common.h"
#include "rw_lock.h"
#include "slice.h"

namespace nap {

class GVView {

	friend class NapMeta;

public:
	// NAL hot set
	std::unordered_map<std::string, Entry> view;

	struct Entry {

		WRLock l; //control concurrent accesses to the NAL
		bool is_deleted;
		bool shifting;

		// used for 3-phase switch for lazy initialization
		WhereIsData location;

		// serve lookup operation
		std::string v;

		// for recovery
		uint64_t version;

		Entry() : is_deleted(false), shifting(false), location(WhereIsData::IN_RAW_INDEX), version(0) { }

		uint64_t next_version() { return version++; }
	};

	GVView() {}

	GVView(const std::vector<std::pair<std::string, WhereIsData>> &list) {
		for(size_t i=0;i<list.size();i++){
			Entry e;
			e.location = list[i].second;
			view[list[i].first] = std::move(e);
		}
	}

	bool get_entry(const Slice &key, Entry *&entry) {	
		auto ret = view.find(jey.toString());
		if(ret == view.end()) {
			return false;
		}

		entry = &ret->second;
		return true;
	}

	// finish lazy initialization
	void relocate_value(GVView *old_view) {
		for(auto e : view){
			if(e.second.location==WhereIsData::IN_PREVIOUS_EPOCH){
				e.second.l.wLock();
				if(e.second.location==WhereIsData::IN_PREVIOUS_EPOCH){
					e.second.location = WhereIsData::IN_CURRENT_EPOCH;
					auto &old_e = old_view->view[e.first];
					e.second.v = old_e.v;
					e.second.is_deleted = old_e.is_deleted;
				}
				e.second.l.wUnlock();
			}
		}
	}

	template <class T>
	void flush_to_raw_index(T *raw_index) {
		for(auto e : view){
			e.second.l.rLock();
			if(!e->is_deleted && e.second.location!=WhereIsData::IN_RAW_INDEX) {
				raw_index->put(Slice(e.first), Slice(e.second.v));
			}
			e.second.l.rUnlock();
		}
	}

};

} // namespace nap

#endif // _GV_VIEW_H_
