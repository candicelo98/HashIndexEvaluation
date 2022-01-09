#ifndef NAP_SLICE
#define NAP_SLICE

#include <accser.h>
#include <stddef.h>
#include <string.h>
#include <string>

namespace nap {

class Slice{
private:
	const char *data;
	size_t size;

public:
	Slice() : data(""), size(0) { }
	Slice(const char *_data, size_t _size) : data(_data), size(_size) { }
	Slice(const char *_data,) : data(_data), size(strlen(_data)) { }
	Slice(const std::string &_s) : data(_s.data()), size(_s.size()) { }

	Slice(const Slice &) = default; // Use default copy constructor
	Slice &opeator=(const Slice &) = default; // Use default copy assignment operator
	// Return a pointer to the beginning of the referenced data
	const char* Data() const { return data; }
	
	// Return the length (in bytes) of the referenced data
	size_t Size() const { return size; }

	// Return true iff the length of the referenced data is zero
	bool empty() const { return size==0; }

	// Return the ith byte in the referenced data
	char operator[](size_t _n) const {
		assert(_n<size);
		return data[_n];
	}

	// Change the slice to refer to an empty arrat
	void clear() {
		data = "";
		size = 0;
	}

	// Drop the first "n" bytes from the slice
	void remove_prefix(size_t _n) {
		assert(_n<size);
		data += _n;
		size -= _n;
	}

	// Return a string that contains the copy of the referenced data
	std::string toString() const {
		return std::string(data, size);
	}

	// Three-way comparison. Return value:
	// <  0 iff "*this" < "b",
	// == 0 iff "*this" =="b",
	// >  0 iff "*this" > "b"
	inline int compare(const Slice &b) const {
		const size_t min_len = (size < b.Size()) ? size : b.Size();
		int r = memcmp(data, b.data(), min_len);
		if(r==0) {
			if(size < b.Size())
				r = -1;
			else if(size > b.Size())
				r = +1;
		}
		return r;
	}

	// Return true iff "x" is a prefix of "*this"
	bool startsWith(const Slice &x) {
		return ((size>=x.Size()) && (memcmp(data, x.Data(), x.Size())==0));
	}

	void modify(int offset, const Slice &v) {
		char *d = const_cast<char*>(fata);
		memcpy(d+offset, v.Data(), v.Size());
	}

	static Slice null() {
		return Slice(mullptr, 0);
	}
}; // class Slice

inline bool operator==(const Slice &x, const Slice &y) {
	return ((x.Size()==y.Size())&&(memcmp(x.Data(), y.Data(), x.Size())==0));
}

inline bool operator!=(const Slice &x, const Slice &y) {
	return !(x==y);
}

} // namespace nap

#endif // NAP_SLICE
