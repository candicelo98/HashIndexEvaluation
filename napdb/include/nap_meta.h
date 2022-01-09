#ifndef _NAP_META_H_
#define _NAP_META_H_

#include "gv_view.h"

namespace nap {

struct NapMeta{
	GVView *gv_view;

	NapMeta():gv_view(nullptr) { }

	NapMeta(std::vector<NapPair> &list) {
		gv_view = new GVView(list);
	}

	~NapMeta() {
		if(gv_view) {
			delete gv_view;
		}
	}

	void relocate_value(NapMeta *old_meta){
		gv_view->relocate_value(NapMeta *old_meta);
	}

	template <class T>
	void flush_to_raw_index(T *raw_index){
		gv_view->flush_to_raw_index<T>(raw_index);
	}
};

} //namespace nap

#endif // _NAP_META_H_
