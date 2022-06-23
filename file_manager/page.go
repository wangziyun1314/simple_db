package file_manager

import "encoding/binary"

type Page struct {
	buffer []byte // 对应内存中的一块数据
}

func NewPageBySize(pageSize uint64) *Page {
	bytes := make([]byte, pageSize)
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

func uint64ToByteArray(val uint64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, val)
	return bytes
}

func (p *Page) SetInt(offset uint64, val uint64) {
	b := uint64ToByteArray(val)
	copy(p.buffer[offset:], b)
}

func (p *Page) GetBytes(offset uint64) []byte {
	len := binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
	newBuffer := make([]byte, len)
	copy(newBuffer, p.buffer[offset+8:])
	return newBuffer
}

func (p *Page) SetBytes(offset uint64, bytes []byte) {
	// 首先写入长度 然后再写入字节数组
	len := len(bytes)
	p.SetInt(offset, uint64(len))
	// 如果buffer剩余的长度小于bytes数组的长度那么后面的数据就会丢失
	copy(p.buffer[offset+8:], bytes)
}

func (p *Page) GetString(offset uint64) string {
	bytes := p.GetBytes(offset)
	return string(bytes)
}

func (p *Page) SetString(offset uint64, s string) {
	bytes := []byte(s)
	p.SetBytes(offset, bytes)
}

func (p Page) MaxLengthForString(s string) uint64 {
	// hello, 世界 长度是13
	bs := []byte(s)
	return uint64(8 + len(bs))
}

func (p *Page) contents() []byte {
	return p.buffer
}
