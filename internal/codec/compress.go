package codec

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"sync"
)

// CompressionType Body 的压缩方式, 与序列化解耦, 由 Header 携带
type CompressionType uint8

const (
	// CompressionNone 不压缩
	CompressionNone CompressionType = iota
	// CompressionGzip gzip 压缩, 只压缩 Body, 不压缩 Header
	CompressionGzip
)

// gzip.NewWriter / NewReader 每次都要分配压缩窗口与哈希表(单次接近 1MB),
// RPC 是高频小包场景, 必须池化复用
var (
	gzipWriterPool = sync.Pool{
		New: func() interface{} {
			return gzip.NewWriter(io.Discard)
		},
	}
	gzipReaderPool sync.Pool // 存 *gzip.Reader, 首次需要数据才能构造, 所以不设 New
)

// Compress 按指定方式压缩数据
func Compress(data []byte, t CompressionType) ([]byte, error) {
	switch t {
	case CompressionNone:
		return data, nil
	case CompressionGzip:
		var buf bytes.Buffer
		// 压缩后一般小于原文, 预留原长度可避免多次扩容
		buf.Grow(len(data))

		w := gzipWriterPool.Get().(*gzip.Writer)
		w.Reset(&buf)

		if _, err := w.Write(data); err != nil {
			gzipWriterPool.Put(w)
			return nil, err
		}
		if err := w.Close(); err != nil {
			gzipWriterPool.Put(w)
			return nil, err
		}
		gzipWriterPool.Put(w)

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

		var (
			r   *gzip.Reader
			err error
		)
		if v := gzipReaderPool.Get(); v != nil {
			r = v.(*gzip.Reader)
			err = r.Reset(bytes.NewReader(data))
		} else {
			r, err = gzip.NewReader(bytes.NewReader(data))
		}
		if err != nil {
			return nil, err
		}

		out, err := io.ReadAll(r)
		closeErr := r.Close()
		gzipReaderPool.Put(r)

		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		return out, nil
	default:
		return nil, fmt.Errorf("codec: unsupported compression type %d", t)
	}
}
