#ifndef _NAP_META_H_
#define _NAP_META_H_

#include "gv_view.h"
#include <algorithm>

namespace nap {

struct NapMeta{
	GVView *gv_view;

	NapMeta():gv_view(nullptr) { }

	NapMeta(std::vector<NapPair> &_list) {
		auto list = _list;
		std::random_shuffle(list.begin(), list.end());
		gv_view = new GVView(list);
	}

	~NapMeta() {
		if(gv_view) {
			delete gv_view;
		}
	}

	void relocate_value(NapMeta *old_meta){
		gv_view->relocate_value(old_meta->gv_view);
	}

	template <class T>
	void flush_gv_view(T *raw_index){
		gv_view->flush_to_raw_index<T>(raw_index);
	}
};

} //namespace nap

#endif // _NAP_META_H_
