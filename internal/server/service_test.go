package server

import (
	"errors"
	"sort"
	"testing"

	"kamaRPC/pkg/api"
)

type noValidMethods struct{}

// 签名不符合 func(req *Req, reply *Resp) error
func (n *noValidMethods) Add(a, b int) int        { return a + b }
func (n *noValidMethods) Nothing()                {}
func (n *noValidMethods) OnlyErr() error          { return nil }
func (n *noValidMethods) NotPtr(a api.Args) error { return nil }

type mixedMethods struct{}

func (m *mixedMethods) Good(args *api.Args, reply *api.Reply) error {
	reply.Result = args.A
	return nil
}
func (m *mixedMethods) AlsoGood(args *api.Args, reply *api.Reply) error { return nil }
func (m *mixedMethods) Bad(a int) error                                 { return errors.New("nope") }

// 注册时就建好方法表, 只收签名合规的方法
func TestNewServiceEntryCollectsValidMethods(t *testing.T) {
	entry, err := newServiceEntry("Mixed", &mixedMethods{})
	if err != nil {
		t.Fatalf("newServiceEntry: %v", err)
	}

	got := entry.MethodNames()
	sort.Strings(got)
	want := []string{"AlsoGood", "Good"}

	if len(got) != len(want) {
		t.Fatalf("got methods %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got methods %v want %v", got, want)
		}
	}

	if _, ok := entry.lookup("Bad"); ok {
		t.Fatal("method with wrong signature should not be registered")
	}
	if m, ok := entry.lookup("Good"); !ok {
		t.Fatal("Good should be registered")
	} else if m.reqType != m.replyType && m.reqType.Name() != "Args" {
		t.Fatalf("unexpected req type %v", m.reqType)
	}
}

// 一个合规方法都没有, 属于用法错误, 必须在注册阶段就报错
func TestRegisterRejectsServiceWithoutUsableMethods(t *testing.T) {
	srv, err := NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	if err := srv.Register("Broken", &noValidMethods{}); err == nil {
		t.Fatal("expected Register to reject a service with no usable methods")
	}

	if err := srv.Register("Arith", &api.Arith{}); err != nil {
		t.Fatalf("Register(Arith): %v", err)
	}
	if err := srv.Register("Arith", &api.Arith{}); err == nil {
		t.Fatal("expected duplicate registration to fail")
	}
}
