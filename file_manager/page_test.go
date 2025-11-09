package file_manager

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSetAndGetInt(t *testing.T) {
	page := NewPageBySize(256)
	val := uint64(1234)
	offset := uint64(23) // 指定位移写入

	page.SetInt(offset, val)

	val_got := page.GetInt(offset)

	require.Equal(t, val, val_got)
}

func TestSetAndGetByteArray(t *testing.T) {
	page := NewPageBySize(256)
	bs := []byte{1, 2, 3, 4, 5, 6}
	offset := uint64(111)

	page.SetBytes(offset, bs)
	bs_got := page.GetBytes(offset)

	require.Equal(t, bs, bs_got)
}

func TestSetAndGetString(t *testing.T) {
	page := NewPageBySize(256)
	s := "hello, 世界"
	offset := uint64(177)
	page.SetString(offset, s)
	s_got := page.GetString(offset)

	require.Equal(t, s, s_got)
}

func TestMaxLengthForString(t *testing.T) {
	s := "hello, 世界"
	s_len := uint64(len([]byte(s)))
	page := NewPageBySize(256)
	s_len_got := page.MaxLengthForString(s)
	require.Equal(t, s_len+8, s_len_got)
}

func TestGetContents(t *testing.T) {
	bs := []byte{1, 2, 3, 4, 5, 6}
	page := NewPageByBytes(bs)
	bs_got := page.contents()

	require.Equal(t, bs, bs_got)
}
