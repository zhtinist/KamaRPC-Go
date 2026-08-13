package api

import "errors"

// Args 示例请求
type Args struct {
	A int
	B int
}

// Reply 示例响应
type Reply struct {
	Result int
}

// Arith 示例服务, 方法签名遵循 net/rpc 风格: func(req *Req, reply *Resp) error
type Arith struct{}

func (a *Arith) Add(args *Args, reply *Reply) error {
	reply.Result = args.A + args.B
	return nil
}

func (a *Arith) Sub(args *Args, reply *Reply) error {
	reply.Result = args.A - args.B
	return nil
}

func (a *Arith) Mul(args *Args, reply *Reply) error {
	reply.Result = args.A * args.B
	return nil
}

func (a *Arith) Div(args *Args, reply *Reply) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	reply.Result = args.A / args.B
	return nil
}

// Arith2 第二个示例服务, 用于演示同一节点注册多个服务
type Arith2 struct{}

func (a *Arith2) Add(args *Args, reply *Reply) error {
	reply.Result = args.A + args.B + 1
	return nil
}

func (a *Arith2) Mul(args *Args, reply *Reply) error {
	reply.Result = args.A * args.B * 2
	return nil
}
