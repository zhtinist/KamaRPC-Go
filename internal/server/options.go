package server

import (
	"errors"

	"kamaRPC/internal/core/codec"
	"kamaRPC/internal/core/limiter"
)

// ServerOption 函数式选项
type ServerOption func(*Server) error

// WithServerCodec 指定服务端序列化协议, 一般与客户端保持一致
func WithServerCodec(t codec.Type) ServerOption {
	return func(s *Server) error {
		cc, err := codec.New(t)
		if err != nil {
			return err
		}
		s.codec = cc
		return nil
	}
}

// WithServerRateLimit 指定服务端限流速率(每秒令牌数)
func WithServerRateLimit(rate int) ServerOption {
	return func(s *Server) error {
		if rate <= 0 {
			return errors.New("server: rate must be positive")
		}
		s.limiter = limiter.NewTokenBucket(rate)
		return nil
	}
}
