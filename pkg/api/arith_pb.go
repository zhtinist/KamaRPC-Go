package api

import (
	"errors"

	"kamaRPC/pkg/api/pb"
)

// ArithPB 与 Arith 功能相同, 但请求/响应用 protobuf 生成的类型。
// 方法签名仍是 net/rpc 风格, 框架侧不需要为 Protobuf 做任何特例处理 ——
// 编解码方式由请求 Header 里的 CodecType 决定
type ArithPB struct{}

func (a *ArithPB) Add(args *pb.Args, reply *pb.Reply) error {
	reply.Result = args.A + args.B
	return nil
}

func (a *ArithPB) Sub(args *pb.Args, reply *pb.Reply) error {
	reply.Result = args.A - args.B
	return nil
}

func (a *ArithPB) Mul(args *pb.Args, reply *pb.Reply) error {
	reply.Result = args.A * args.B
	return nil
}

func (a *ArithPB) Div(args *pb.Args, reply *pb.Reply) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	reply.Result = args.A / args.B
	return nil
}

// Big 是 pb.Big 的 JSON 对等结构体, 用于对比两种编码在大包下的开销
type Big struct {
	Values []int64
	Text   string
}
