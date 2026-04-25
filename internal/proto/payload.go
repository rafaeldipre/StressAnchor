package proto

import "bytes"

func Payload(size int) []byte {
	if size <= 0 {
		return nil
	}
	pattern := []byte("StressAnchor0123456789")
	return bytes.Repeat(pattern, (size/len(pattern))+1)[:size]
}

func Equal(a, b []byte) bool {
	return bytes.Equal(a, b)
}
