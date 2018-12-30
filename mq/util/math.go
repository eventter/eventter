package util

// See http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
func NextPowerOfTwo32(n uint32) uint32 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}
