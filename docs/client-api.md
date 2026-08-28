# gcs-pool 客户对接文档

客户只需要做一件事：**把文件 POST 到上传接口，拿回一个可直接访问的 URL**。
不需要任何 Google 账号、密钥、bucket 概念。

## 快速开始

### 上传接口

```
POST {服务地址}/upload
Authorization: Bearer <api_key>       ← 必填，提供方签发
Content-Type: multipart/form-data
字段: file = 文件
```

`api_key` 由提供方在管理后台签发（格式 `gcs-...`），一个 key 可供一个或多个客户端使用，吊销即失效。

服务地址由提供方告知（示例：`https://upload.example.com:11223` 或内网地址）。

### Python 示例

```python
import requests

resp = requests.post(
    "https://upload.example.com/upload",
    headers={"Authorization": "Bearer <api_key>"},
    files={"file": open("local_image.png", "rb")},
    timeout=1800,  # 大文件给足超时
)
data = resp.json()

print(data["uri"])        # 签名 URL，直接浏览器/下载器可访问
print(data["mimeType"])   # 如 image/png
print(data["name"])       # 原始文件名
```

### curl 示例

```bash
curl -F "file=@/path/to/local_image.png" \
  -H "Authorization: Bearer <api_key>" \
  https://upload.example.com/upload
```

### JS 示例（浏览器，带进度）

```js
const xhr = new XMLHttpRequest();
xhr.open('POST', '/upload');
xhr.upload.onprogress = e => console.log(`${Math.round(e.loaded / e.total * 100)}%`);
xhr.onload = () => {
  const data = JSON.parse(xhr.responseText);
  console.log(data.url); // 签名 URL
};
const fd = new FormData();
fd.append('file', fileInput.files[0]);
xhr.send(fd);
```

## 响应格式

成功（HTTP 200）：

```json
{
  "uri": "https://storage.googleapis.com/xxx/2026/08/28/1604-abc123.png?X-Goog-Algorithm=...&X-Goog-Signature=...",
  "mimeType": "image/png",
  "name": "local_image.png",
  "bucket": "bucket-name",
  "object": "2026/08/28/1604-abc123.png",
  "size": 123456
}
```

- `uri`：**签名 URL，7 天有效**（含签名参数，不需要任何登录/凭证即可下载）。过期后需重新上传。
- `mimeType`：文件类型；`name`：原始文件名
- `bucket` / `object` / `size`：存储信息（一般用不到）

## 错误码

| HTTP | 含义 | 处理建议 |
|---|---|---|
| 400 | 请求格式错误（非 multipart / 缺 file 字段） | 检查请求体 |
| 401 | 鉴权失败（如配置了访问凭证） | 联系提供方 |
| 413 | 文件超过大小上限（默认 1GB，可配） | 分片或联系提供方调上限 |
| 502 | 上传失败（存储侧异常/重试耗尽） | 重试；持续失败联系提供方 |
| 503 | 服务繁忙（并发超限排队超时） | 稍后重试 |

失败时返回：`{"error": "错误描述"}`

## 注意事项

- **大小上限**：单文件默认 1GB（提供方可配置），超大文件联系提供方
- **上传超时**：服务端默认 30 分钟超时，客户端 timeout 建议给足
- **并发**：可并发上传，服务端自动排队限流；高峰期可能返回 503，重试即可
- **断点续传**：上传中断需整文件重传（服务端存储链路有自动重试，网络抖动一般不会失败）
- **文件名**：服务端自动生成存储路径（日期+随机名），原始文件名仅作记录，不会暴露在 URL 中

## 健康检查

```
GET {服务地址}/healthz
```

返回 `{"status":"ok", ...}` 即服务正常。
