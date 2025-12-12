package util

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func IsIntegerStrict(s string) bool {
	// 基础10进制，64位整数范围
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func ClearPath(dirPath string) error {
	return os.RemoveAll(dirPath)
}

func Error(format string, args ...interface{}) error {
	return errors.New(fmt.Sprintf(format, args...))
}

var (
	Mismatch      = errors.New("mismatch")
	WriteConflict = errors.New("WriteConflict")
)

func Join(names []string, s string) string {
	return strings.Join(names, s)
}

func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
