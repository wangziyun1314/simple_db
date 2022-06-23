package file_manager

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSetAndGetInt(t *testing.T) {
	page := NewPageBySize(256)
	val := uint64(4545)
	offset := uint64(34)
	page.SetInt(offset, val)
	getInt := page.GetInt(offset)
	require.Equal(t, val, getInt)
}

func TestGetAndSetBytes(t *testing.T) {
	p := NewPageBySize(256)
	offset := uint64(56)
	bytes := []byte{2, 3, 4, 6, 0, 8, 2, 7, 9, 8, 2, 0, 9, 4, 0, 6, 4, 5, 7}
	p.SetBytes(offset, bytes)
	getBytes := p.GetBytes(offset)
	require.Equal(t, bytes, getBytes)
}

func TestGetAndSetString(t *testing.T) {
	p := NewPageBySize(345)
	offset := uint64(43)
	s := "我爱你中国"
	p.SetString(offset, s)
	getString := p.GetString(offset)
	require.Equal(t, s, getString)
}

func TestPage_MaxLengthForString(t *testing.T) {

	s := "hello, 世界"
	sLen := uint64(len([]byte(s)))
	p := NewPageBySize(256)
	forString := p.MaxLengthForString(s)
	require.Equal(t, sLen+8, forString)
}

func TestGetContents(t *testing.T) {
	bs := []byte{1, 2, 3, 4, 5, 6}
	p := NewPageByBytes(bs)
	contents := p.contents()
	require.Equal(t, bs, contents)
}
