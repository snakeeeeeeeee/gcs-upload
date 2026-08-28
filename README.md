# gcs-pool — GCS 号池上传服务

多 GCS service account key 组成号池，`round-robin` 轮询选号上传文件，失败自动换号重试，返回 **v4 签名 URL**（私有 bucket 也能匿名访问）。

**客户对接**：对外只需一个 `POST /upload` 接口，客户传文件拿签名 URL 即可，GCS 全部细节由服务内部处理。对接文档见 [`docs/client-api.md`](docs/client-api.md)。

## 架构

```
客户端 --POST /upload--> Go 服务 --选号(轮询/换号重试)--> GCS 客户端池(key#1..N) --> Google Cloud Storage
                                                                                          │
                                                      返回 v4 签名 URL（7 天有效，私有桶可访问）
```

## 配置

复制 `config.example.json` 为 `config.json`：

| 字段 | 说明                                                                |
|---|-------------------------------------------------------------------|
| `listen` | 监听地址，默认 `:11223`                                                  |
| `default_bucket` | 默认 bucket（**可选**），未配 bucket 的号用它；留空则自动用项目第一个桶，项目没桶则**自动创建**        |
| `max_size` | 单文件上限（**MB**），默认 1024（1GB）；**管理台系统信息可实时修改并写回 config**              |
| `max_concurrent` | 全局并发上传上限，默认 1024；**管理台可实时修改（动态信号量支持扩缩）**        |
| `request_timeout` | 上传请求超时（秒），默认 1800（30 分钟）；**管理台可实时修改**                  |
| `retry` | 普通失败换号重试次数，默认 3；**管理台可实时修改**                              |
| `retry` | 普通失败换号重试次数，默认 3                                                   |
| `max_concurrent` | 全局并发上传上限，超限排队，默认 1024                                             |
| `request_timeout` | 单个上传请求超时（秒），默认 1800（30 分钟）                                        |
| `retry_429_base` | GCS 限流（429）退避基础秒数，指数增长，默认 1                                       |
| `retry_429_max` | 429 最大退避次数，默认 5                                                   |
| `signed_url_ttl` | 返回签名 URL 有效期（秒），默认 604800（7 天，v4 官方上限）；`<=0` 返回原生地址（需 bucket 公开读） |
| `ttl_days` | 全局存储期限（天），默认 7，所有号统一；到期自动删除，客户不可改；`<=0` 永久存储；**管理台系统信息可实时修改并写回 config** |
| `bucket_location` | 自动创建桶的区域，默认 `US`（如 `asia-east1`、`europe-west1`）                    |
| `health_check_interval` | 健康检查间隔（秒），默认 300（5 分钟），后台定期探活每个 key；`0` 关闭                        |
| `keys_scan_interval` | keys/ 目录自动扫描间隔（秒），默认 60，丢进目录的新 key JSON 自动注册上线                    |
| `admin_password` | 管理登录密码（必填），登录页使用（兼容旧字段 `admin_token`）                             |
| `api_keys` | 客户端 API Key 列表（`Authorization: Bearer <key>` 调 `/upload`），管理页可生成/删除          |
| `accounts[].key_file` | SA key JSON 路径（必填，相对 config 目录解析）                                 |
| `accounts[].bucket` | 可选，覆盖默认 bucket（每个号可绑自己的 bucket；留空走默认/自动逻辑）                     |
| `accounts[].name` | 可选，号名（日志用）                                                        |
| `accounts[].enabled` | 可选，初始是否禁用（默认启用，管理页可切换）                                            |

## 运行

```bash
go run . -config config.json
# 或直接跑二进制
./gcs-pool -config config.json
```

### Docker 运行

```bash
docker compose up -d --build
```

- 多阶段构建，镜像**不含任何配置/key**（敏感内容不进镜像）
- `config.json` 和 `keys/` 目录挂载进容器，管理页账号变更写回宿主机文件，重启不丢
- 容器日志走 stdout：`docker logs -f gcs-pool`
- 端口映射 `11223:8090`（改 compose 左侧端口即可）
- **直连 GCS**：无代理配置，直接部署在海外服务器即可使用

启动后浏览器打开 `http://localhost:8090/`，会先跳到独立登录页 `http://localhost:8090/login`，输入 `admin_password` 进入管理台（httpOnly 会话 cookie，24h 有效，可退出）。

### 辅助工具：SA Key 创建桶权限探测

新号加入前可用 `tools/probe-create` 四步实测该 key 的权限全链路（创建 → 配 TTL → 上传 → 清理，跑完自动删除测试桶无残留）：

```bash
go run ./tools/probe-create keys/acc-3.json [location]   # location 默认 US
# 输出示例：
# [1/4] CREATE OK    bucket=gcs-pool-probe-xxx project=my-project location=US
# [2/4] UPDATE OK    生命周期规则(7d) 已写入
# [3/4] UPLOAD OK    probe.txt 上传成功 (创建者自动为 bucket owner)
# [4/4] CLEANUP OK   测试桶已删除, 无残留
```

结论：四步全过 = 该 key 具备"自动创建桶"能力，程序 bucket 留空时可直接用（缺 `storage.buckets.create` 时会在 [1/4] 报 403，需到 GCP 控制台给 SA 授 `roles/storage.admin`）。

## 管理台（Web UI）

内嵌单页（`go:embed`，暗色主题），独立登录页 `/login`，无需单独部署。功能：

- **上传测试**：拖拽上传 + 进度条 + 结果卡片（URL 一键复制），选择 API Key 模拟客户端，可**指定号/指定 bucket** 诊断单个 key
- **号池管理**：查看每个号的状态（在线/熔断/禁用）与**主动健康监测**（后台定期探活每个 key，正常/异常/未测徽章 + 最近检查时间 + 错误详情）、上传/失败计数、最近错误，**每行显示实际使用的 bucket**（自动创建的带 ⚡ 标记）；**添加号**（模态框填名称 + 粘贴 key JSON + 可选 bucket，留空=自动用项目第一个桶/无则自动创建，热加载不重启）、移除、启用/禁用
- **API Keys**：生成/复制/删除客户端 API Key（`gcs-` 前缀），上传接口鉴权用
- **统计**：账号数/启用数/熔断数/总上传/成功率 + 每号用量分布
- **上传记录**：最近 10 条（成功/失败、尝试链、错误原因）
- **系统信息**：默认 bucket、单文件上限、重试次数

**持久化**：管理页的账号变更会立即写回 `config.json`（key 内容落盘到 `keys/{name}.json`，权限 0600），重启后保持。初始账号在 `accounts` 里直接配 `key_file` 路径即可。

## API

### 上传（需 API Key 鉴权）

```bash
curl -F "file=@/path/to/local.jpg" \
  -H "Authorization: Bearer <api_key>" \
  http://localhost:11223/upload
```

`api_key` 在管理台 **API Keys** 面板生成（`gcs-` 前缀）。响应（对齐客户 demo 字段 `uri`/`mimeType`/`name`）：

```json
{
  "uri": "https://storage.googleapis.com/...?...",
  "bucket": "my-bucket",
  "object": "2026/08/27/142705-8f3a2c1b.jpg",
  "mimeType": "image/jpeg",
  "name": "local.jpg",
  "size": 123456
}
```

`url` 即 **v4 签名 URL**（默认 7 天有效，含 `X-Goog-Signature` 参数），**私有 bucket 也能匿名直接访问**，无需公开读。超过有效期后链接失效，需重新上传或重新签名。

> v4 签名 URL 官方硬限制：过期时间最长 7 天，配置超出会自动钳制到 7 天。需要更长有效期可设 `signed_url_ttl <= 0` 返回原生地址（需 bucket 公开读）。

### 上传 Demo（Python / Go / JS / curl）

统一参数：`POST /upload`，multipart 字段 `file`，Header 带 `Authorization: Bearer <api_key>`。

**Python（requests）**

```python
import requests

resp = requests.post(
    "http://localhost:11223/upload",
    headers={"Authorization": "Bearer <api_key>"},
    files={"file": open("local.jpg", "rb")},
    timeout=1800,  # 大文件给足超时
)
data = resp.json()
print(data["uri"])        # 签名 URL，直接可访问
print(data["mimeType"])   # image/jpeg
print(data["name"])       # local.jpg
```

**Go**

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
)

func main() {
	file, _ := os.Open("local.jpg")
	defer file.Close()

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, _ := w.CreateFormFile("file", "local.jpg")
	io.Copy(fw, file)
	w.Close()

	req, _ := http.NewRequest("POST", "http://localhost:11223/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Authorization", "Bearer <api_key>")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	var data struct {
		URI      string `json:"uri"`
		MimeType string `json:"mimeType"`
		Name     string `json:"name"`
	}
	json.NewDecoder(resp.Body).Decode(&data)
	fmt.Println(data.URI)
}
```

**JS（浏览器，带进度）**

```js
const xhr = new XMLHttpRequest();
xhr.open('POST', 'http://localhost:11223/upload');
xhr.setRequestHeader('Authorization', 'Bearer <api_key>');
xhr.upload.onprogress = e => {
  if (e.lengthComputable) console.log(Math.round(e.loaded / e.total * 100) + '%');
};
xhr.onload = () => {
  const data = JSON.parse(xhr.responseText);
  console.log(data.uri); // 签名 URL
};
const fd = new FormData();
fd.append('file', fileInput.files[0]); // 选择文件后
xhr.send(fd);
```

**curl**

```bash
curl -F "file=@local.jpg" \
  -H "Authorization: Bearer <api_key>" \
  http://localhost:11223/upload
```

可选 header 指定号/指定 bucket（管理页诊断用）：

```bash
curl -F "file=@a.jpg" \
  -H "X-GCS-Account: acc-2" \
  -H "X-GCS-Bucket: my-other-bucket" \
  http://localhost:8080/upload
```

### 存储期限（TTL，到期自动删除）

**由服务方统一配置**（`config.json` 的 `ttl_days`，默认 7 天），**客户端不能修改**——所有文件到期自动删除：

```json
{ "ttl_days": 7 }   // 全部文件 7 天后自动删除；<=0 = 永久存储
```

- 所有上传对象名带 `{n}d/` 前缀（如 `7d/2026/08/28/...`）
- **前置**：bucket 需配置对应前缀的生命周期规则。管理台「系统信息 → 生命周期配置」可自动配置（用号池里有权限的号），或 GCS 控制台手动配
- 规则示例：前缀 `7d/` 的对象 7 天后删除（Age=7, matchesPrefix=7d/）
- ⚠️ bucket 未配置对应规则时对象**不会自动删除**（永久保留）；生命周期删除是 GCS 后台任务，实际删除最长约 24h 延迟

### 健康检查（公开）

```bash
curl http://localhost:8080/healthz
```

### 管理 API（Bearer token 鉴权）

| 接口 | 方法 | 说明 |
|---|---|---|
| `/admin/login` | POST | 登录 `{token}`，下发 httpOnly 会话 cookie（24h） |
| `/admin/logout` | POST | 登出，清除会话 |
| `/admin/pool` | GET | 号池状态 + 统计 |
| `/admin/pool/accounts` | POST | 添加号 `{name, bucket?, key_json}`（写回 config + 落盘 key） |
| `/admin/pool/accounts/{name}` | DELETE | 移除号（写回 config + 清理 key 文件） |
| `/admin/pool/accounts/{name}/{action}` | POST | `enable` / `disable`（写回 config） |
| `/admin/config` | POST | 修改默认 bucket `{default_bucket}`（写回 config） |
| `/admin/records?limit=10` | GET | 上传记录（内存保留最近 10 条，重启清空） |

管理 API 鉴权：浏览器走会话 cookie；脚本/curl 走 `Authorization: Bearer <token>` 或 `X-Admin-Token`。

## 机制

- **选号**：原子计数器 round-robin，跳过已禁用/熔断中的号
- **换号重试**：上传失败自动换下一个号，最多 `retry` 次，错误信息带尝试链（如 `tried [acc-1 acc-2 acc-1]`）
- **熔断**：某号连续失败 5 次自动熔断，冷却 60s 后自动恢复
- **429 退避**：GCS 限流（HTTP 429 / gRPC ResourceExhausted）时指数退避（1s, 2s, 4s...），最多 `retry_429_max` 次后放弃，不消耗普通重试次数
- **签名 URL**：上传成功后用该号私钥生成 v4 签名 URL（默认 7 天），私有 bucket 无需开公开读；`signed_url_ttl<=0` 时回退为原生地址
- **健康监测**：后台按 `health_check_interval` 定期探活每个 key（列对象验证连通+权限，空桶也算健康），结果实时展示在管理页；探活需要 `storage.objects.list` 权限，失败会显示具体错误
- **keys 目录自动扫描**：后台按 `keys_scan_interval` 扫描 `keys/` 目录，发现新 key JSON 自动注册上线（校验 → 落盘 → 进池 → 写回 config），已注册的跳过；bucket 走四级解析，解析失败会跳过并在日志说明原因
- **并发限流**：全局信号量限制并发上传（默认 1024，可配），超出排队；排队长过 `request_timeout` 返回 503
- **请求超时**：整个上传受 `request_timeout` 约束（默认 30 分钟），超时中止
- **热插拔 + 持久化**：管理页添加/移除/启停号即时生效，同时写回 `config.json`（key 落盘 `keys/`，原子写防并发），重启保持
- **key 路径**：`key_file` 相对路径按 config.json 所在目录解析，迁移整个目录即可
- **bucket 四级解析（自动创建）**：每个号的 bucket 按「号级 `accounts[].bucket` → `default_bucket` → 项目第一个桶（按名排序）→ **自动创建**」取优先级。**只给 SA 授 `roles/storage.admin`（Storage Admin）即可，有桶自动用第一个、没桶自动建一个**（创建者自动成为 bucket owner，无需再授权；创建时直接套用全局 `ttl_days` 生命周期规则）。管理页号池表格用 ⚡ 标记自动创建的桶
- **日志**：实时输出到 console（stderr），不写文件；失败重试/退避/熔断均有告警

## 注意事项

- **私有 bucket + 签名 URL**：默认返回 v4 签名 URL（7 天），bucket 保持私有即可，无需开公开读。只有把 `signed_url_ttl` 设为 `<=0` 时才返回原生地址，那时代码里需要给 bucket 授权 `allUsers` 读取。
- **object 名自动生成**：日期目录 + 时间戳 + 随机后缀，规避 GCS 同 object 名 1 写/秒的限制。
- **直连 GCS**：服务直连 `storage.googleapis.com`（无内置代理），部署在海外服务器即可正常使用；国内网络环境需自行处理网络可达性（如整机代理），服务本身不做代理配置。
- **端口冲突**：确保 `listen` 端口未被占用（如本机 Docker 服务）。
- **上传方式**：Go SDK 对 >16MiB 文件自动走可续传上传，断点自动重试；本服务将请求体先落临时文件，失败换号时重新读流，保证重试正确性。
- **SA 权限**：号池 key 只需对象级权限（`roles/storage.objectCreator` 即可上传）；管理页显示/删对象需要更高权限（`roles/storage.objectAdmin`）。
