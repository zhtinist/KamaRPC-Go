package codec

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// CompressionType Body 的压缩方式, 与序列化解耦, 由 Header 携带
type CompressionType uint8

const (
	// CompressionNone 不压缩
	CompressionNone CompressionType = iota
	// CompressionGzip gzip 压缩, 只压缩 Body, 不压缩 Header
	CompressionGzip
)

// Compress 按指定方式压缩数据
func Compress(data []byte, t CompressionType) ([]byte, error) {
	switch t {
	case CompressionNone:
		return data, nil
	case CompressionGzip:
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			w.Close()
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("codec: unsupported compression type %d", t)
	}
}

// Decompress 按指定方式解压数据
func Decompress(data []byte, t CompressionType) ([]byte, error) {
	switch t {
	case CompressionNone:
		return data, nil
	case CompressionGzip:
		if len(data) == 0 {
			return data, nil
		}
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	default:
		return nil, fmt.Errorf("codec: unsupported compression type %d", t)
	}
}
