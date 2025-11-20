package util

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

func IsIntegerStrict(s string) bool {
	// 基础10进制，64位整数范围
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func ClearPath(dirPath string) {
	os.RemoveAll(dirPath)
}

func Error(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

var (
	Mismatch      = errors.New("Mismatch")
	WriteConflict = errors.New("WriteConflict")
)
