package arch

import (
	"go/build"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 分层规则:
//
//	core   —— 客户端与服务端共享的通信内核, 不认识上层的任何东西
//	client —— 调用端(含它专属的负载均衡与熔断)
//	server —— 服务端运行时
//
// 依赖只能从 client/server 指向 core, client 与 server 之间互不依赖。
// 这条规则靠这个测试固化: 有人写反了依赖方向, 这里会直接失败
func TestLayering(t *testing.T) {
	const modulePrefix = "kamaRPC/internal/"

	rules := []struct {
		layer     string   // 这一层的包路径前缀
		forbidden []string // 不允许依赖的层
	}{
		{layer: "core", forbidden: []string{"client", "server"}},
		{layer: "client", forbidden: []string{"server"}},
		{layer: "server", forbidden: []string{"client"}},
	}

	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}

	checked := 0
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || !info.IsDir() {
			return err
		}
		if info.Name() == "arch" {
			return filepath.SkipDir
		}

		pkg, err := build.ImportDir(path, 0)
		if err != nil {
			// 目录里没有 Go 文件, 跳过
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		layer := strings.SplitN(filepath.ToSlash(rel), "/", 2)[0]

		imports := append(append([]string{}, pkg.Imports...), pkg.TestImports...)
		imports = append(imports, pkg.XTestImports...)

		for _, rule := range rules {
			if layer != rule.layer {
				continue
			}
			checked++
			for _, imp := range imports {
				if !strings.HasPrefix(imp, modulePrefix) {
					continue
				}
				dep := strings.TrimPrefix(imp, modulePrefix)
				for _, bad := range rule.forbidden {
					if dep == bad || strings.HasPrefix(dep, bad+"/") {
						t.Errorf("%s 依赖了 %s: %s 层不允许依赖 %s 层",
							filepath.ToSlash(rel), imp, rule.layer, bad)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if checked == 0 {
		t.Fatal("一个包都没检查到, 说明这个测试本身失效了")
	}
	t.Logf("已检查 %d 个包的依赖方向", checked)
}
