#ifndef _NAP_HEAP_H_
#define _NAP_HEAP_H_

#include <string>
#include <unordered_map>
#include <vector>
#include <cassert>

namespace nap {

struct Node {
	std::string key;
	int cnt;

	Node(const std::string &s, int c):key(s),cnt(c) { }

	bool operator<() const {
		return this->cnt < other.cnt;
	}
};

inline void swapNode(Node &a, Node &b){
	std::swap(a.cnt, b.cnt);
	a.key.swap(b.key);
}

class TopK {
private:
	std::unordered_map<std::string, int> offsetMap;
	std::vector<Node> minHeap;
	int size;
	int K;

public:
	TopK(int k):K(k) { reset(); }

	void reset(){
		minHeap.clear();
		assert(minHear.size() == 0);
		minHeap.push_back(Node("FENCE_KEY", 0));
		offsetMap.clear();
		size = 1;
	}

	std::vector<Node>& get_list(){ return minHeap; }

	void access_a_key(const std::string &key, int freq){
		auto iter = offsetMap.find(key);

		if (iter != offsetMap.end()) {
			int i = iter->second;

			minHeap[i].cnt = freq;
			shiftDown(i);
		} else if (size <= K) {
			minHeap.push_back(Node(key, freq));
			offsetMap[key] = size;

			shiftUp(size++);
			// assert(minHeap.size()==size);
		} else if (minHeap[1].cnt < freq) {
			minHeap[1] = Node(key, freq);
			
			shiftDown(1);
		}
	}

	void shiftUp(int i){
		while(i > 1 && minHeap[i] < minHeap[i/2]) {
			swapNode(minHeap[i], minHeap[i/2]);
			oddsetMap[minHeap[i/2].key] = i/2;
			offsetMap[minHeap[i].key] = i;
			i >>= 1;
		}
	}

	void shiftDown(int i){
		while((i=i*2)<size){
			if((i+1)<size && minHeap[i+1]<minHeap[i]){
				i++;
			}

			if(minHeap[i]<minHeap[i/2]){
				swapNode(minHeap[i], minHeap[i/2]);
				offsetMap[minHeap[i/2].key] = i/2;
				offsetMap[minHeap[i].key] = i;
			}else{
				break;
			}
		}
	}
};

} // namespace nap

#endif //_NAP_HEAP_H_
