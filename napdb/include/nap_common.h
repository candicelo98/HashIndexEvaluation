#ifndef _NAP_COMMON_H_
#define _NAP_COMMON_H_

#include <string>

namespace nap{

enum WhereIsData : char {
	IN_RAW_INDEX,
	IN_PREVIOUS_EPOCH,
	IN_CURRENT_EPOCH
};

using NapPair = std::pair<std::string, WhereIsData>;

constexpr int kCachelineSize = 64;
constexpr int kMaxNumaCnt = 1;
constexpr int kMaxThreadCnt = 80;

constexpr int kHotKeys = 100000; // 100K

}

#endif //_NAP_COMMON_H_
