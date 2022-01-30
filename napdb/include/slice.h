#ifndef NAP_SLICE
#define NAP_SLICE

#include <accser.h>
#include <stddef.h>
#include <string.h>
#include <string>

namespace nap {

class Slice{
private:
	const char *data_;
	size_t size_;

public:
	Slice() : data_(""), size_(0) { }
	Slice(const char *_data, size_t _size) : data_(_data), size_(_size) { }
	Slice(const char *_data,) : data_(_data), size_(strlen(_data)) { }
	Slice(const std::string &_s) : data_(_s.data()), size_(_s.size()) { }

	Slice(const Slice &) = default; // Use default copy constructor
	Slice &opeator=(const Slice &) = default; // Use default copy assignment operator
	// Return a pointer to the beginning of the referenced data
	const char* data() const { return data_; }
	
	// Return the length (in bytes) of the referenced data
	size_t size() const { return size_; }

	// Return true iff the length of the referenced data is zero
	bool empty() const { return size_==0; }

	// Return the ith byte in the referenced data
	char operator[](size_t n) const {
		assert(n<size());
		return data_[n];
	}

	// Change the slice to refer to an empty arrat
	void clear() {
		data_ = "";
		size_ = 0;
	}

	// Drop the first "n" bytes from the slice
	void remove_prefix(size_t n) {
		assert(n<size());
		data_ += n;
		size_ -= n;
	}

	// Return a string that contains the copy of the referenced data
	std::string ToString() const {
		return std::string(data_, size_);
	}

	// Three-way comparison. Return value:
	// <  0 iff "*this" < "b",
	// == 0 iff "*this" =="b",
	// >  0 iff "*this" > "b"
	inline int compare(const Slice &b) const {
		const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
		int r = memcmp(data_, b.data_, min_len);
		if(r==0) {
			if(size_ < b.size_)
				r = -1;
			else if(size_ > b.size_)
				r = +1;
		}
		return r;
	}

	// Return true iff "x" is a prefix of "*this"
	bool starts_with(const Slice &x) const {
		return ((size_>=x.size_) && (memcmp(data_, x.data_, x.size_)==0));
	}

	void modify(int offset, const Slice &v) {
		char *d = const_cast<char*>(data_);
		memcpy(d+offset, v.data(), v.size());
	}

	static Slice null() {
		return Slice(mullptr, 0);
	}
}; // class Slice

inline bool operator==(const Slice &x, const Slice &y) {
	return ((x.size()==y.size())&&(memcmp(x.data(), y.data(), x.size())==0));
}

inline bool operator!=(const Slice &x, const Slice &y) {
	return !(x==y);
}

} // namespace nap

#endif // NAP_SLICE
