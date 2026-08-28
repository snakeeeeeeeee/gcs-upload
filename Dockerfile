# ---------- 构建阶段 ----------
FROM golang:1.25-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# 静态编译，-s -w 精简体积
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o gcs-pool .

# ---------- 运行阶段 ----------
FROM alpine:3.20
# CA 证书（HTTPS 必需）+ 时区
RUN apk add --no-cache ca-certificates tzdata \
    && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone
WORKDIR /app
COPY --from=builder /build/gcs-pool .
# 配置目录（挂载卷：宿主机 config.json + keys/ 映射到这里）
RUN mkdir -p /app/config
EXPOSE 8090
ENTRYPOINT ["./gcs-pool", "-config", "/app/config/config.json"]
