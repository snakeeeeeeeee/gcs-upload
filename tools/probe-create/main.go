// probe-create 探测某个 GCS service account key 是否具备"创建桶"能力。
//
// 用法:
//
//	go run ./tools/probe-create keys/acc-2.json [location]
//
// 依次实测四步, 每步独立报告:
//  1. Create        -> 需要 storage.buckets.create      (项目级 IAM)
//  2. UpdateAttrs   -> 需要 storage.buckets.update      (配置 TTL 生命周期)
//  3. Upload object -> 需要 storage.objects.create      (上传文件)
//  4. Cleanup       -> 删除测试对象与测试桶, 不留垃圾
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: probe-create <key-file.json> [location]")
		os.Exit(1)
	}
	keyFile := os.Args[1]
	loc := "US"
	if len(os.Args) >= 3 {
		loc = os.Args[2]
	}

	raw, err := os.ReadFile(keyFile)
	if err != nil {
		fatal("read key file: %v", err)
	}
	var kf struct {
		ProjectID string `json:"project_id"`
	}
	if err := json.Unmarshal(raw, &kf); err != nil {
		fatal("parse key file: %v", err)
	}
	if kf.ProjectID == "" {
		fatal("key file has no project_id")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := storage.NewClient(ctx, option.WithCredentialsFile(keyFile))
	if err != nil {
		fatal("storage client: %v", err)
	}
	defer client.Close()

	name := fmt.Sprintf("gcs-pool-probe-%d", time.Now().UnixNano())
	bkt := client.Bucket(name)

	// 1) 创建桶 (不含 lifecycle, 单独测 create 权限)
	if err := bkt.Create(ctx, kf.ProjectID, &storage.BucketAttrs{Location: loc}); err != nil {
		fatal("CREATE FAILED (无 storage.buckets.create 权限): %v", err)
	}
	fmt.Printf("[1/4] CREATE OK     bucket=%s project=%s location=%s\n", name, kf.ProjectID, loc)

	// 2) 给桶追加 TTL 生命周期规则 (测 update 权限, 对应管理页"一键应用 TTL")
	if _, err := bkt.Update(ctx, storage.BucketAttrsToUpdate{
		Lifecycle: &storage.Lifecycle{Rules: []storage.LifecycleRule{
			{Action: storage.LifecycleAction{Type: storage.DeleteAction}, Condition: storage.LifecycleCondition{AgeInDays: 7}},
		}},
	}); err != nil {
		fatal("UPDATE FAILED (无 storage.buckets.update 权限, 但 create 已通过): %v", err)
	}
	fmt.Println("[2/4] UPDATE OK     生命周期规则(7d) 已写入")

	// 3) 上传一个测试对象
	obj := bkt.Object("probe.txt")
	w := obj.NewWriter(ctx)
	if _, err := io.WriteString(w, "probe"); err != nil {
		fatal("UPLOAD FAILED (无 storage.objects.create 权限): %v", err)
	}
	if err := w.Close(); err != nil {
		fatal("UPLOAD FAILED (close): %v", err)
	}
	fmt.Println("[3/4] UPLOAD OK     probe.txt 上传成功 (创建者自动为 bucket owner)")

	// 4) 清理: 删对象 + 删桶
	if err := obj.Delete(ctx); err != nil {
		fmt.Printf("[4/4] WARN 删除测试对象失败(请手动清理 %s): %v\n", name, err)
	} else if err := bkt.Delete(ctx); err != nil {
		fmt.Printf("[4/4] WARN 删除测试桶失败(请手动清理 %s): %v\n", name, err)
	} else {
		fmt.Printf("[4/4] CLEANUP OK    测试桶 %s 已删除, 无残留\n", name)
	}

	fmt.Println("\n结论: 该 key 具备创建桶全链路权限, 程序可集成\"一键创建桶\"功能")
}

func fatal(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
	os.Exit(1)
}
