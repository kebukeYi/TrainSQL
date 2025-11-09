package file_manager

import (
	"encoding/binary"
)

// Page 保存的是 blockIndex 索引块所指向的文件数据字节内容; 仅在内存中存在;
type Page struct {
	buffer []byte
}

func NewPageBySize(block_size uint64) *Page {
	bytes := make([]byte, block_size)
	return &Page{
		buffer: bytes,
	}
}

func NewPageByBytes(bytes []byte) *Page {
	return &Page{
		buffer: bytes,
	}
}

func (p *Page) GetInt(offset uint64) uint64 {
	num := binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
	return num
}

func (p *Page) SetInt(offset uint64, val uint64) {
	b := uint64ToByteArray(val)
	copy(p.buffer[offset:], b)
}

func uint64ToByteArray(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}

func (p *Page) SetBytes(offset uint64, b []byte) {
	// 1. 先将长度写入前8字节;
	// 2. 再将二进制数据写入;
	len_buf := uint64ToByteArray(uint64(len(b)))
	copy(p.buffer[offset:], len_buf)
	copy(p.buffer[offset+8:], b)
}

func (p *Page) GetBytes(offset uint64) []byte {
	// 前8字节表示后续二进制数据长度;
	lengh := binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
	new_buf := make([]byte, lengh)
	// 因此需要跳过8个字节;
	copy(new_buf, p.buffer[offset+8:])
	return new_buf
}

func (p *Page) SetString(offset uint64, s string) {
	str_bytes := []byte(s)
	p.SetBytes(offset, str_bytes)
}

func (p *Page) GetString(offset uint64) string {
	str_bytes := p.GetBytes(offset)
	return string(str_bytes)
}

func (p *Page) MaxLengthForString(s string) uint64 {
	uint64_size := 8 // 存储字符串时预先存储其长度，也就是uint64,它占了8个字节
	bs := []byte(s)  // 返回字符串相对于字节数组的长度
	return uint64(uint64_size + len(bs))
}

func (p *Page) contents() []byte {
	return p.buffer
}
