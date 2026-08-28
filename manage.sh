#!/usr/bin/env bash
# gcs-pool 服务器一键管理脚本
# 用法: ./manage.sh {init|start|restart|status|logs|down}
#
#   init    首次部署：生成 config.json（随机管理密码并打印）+ 启动
#   start   构建并启动容器
#   restart 强制重建并重启容器（改代码/配置后常用）
#   status  容器状态 + healthz + 最近日志
#   logs    跟踪日志
#   down    停止并移除容器

set -euo pipefail
cd "$(dirname "$0")"

CONFIG="config.json"
COMPOSE="docker-compose.yml"
CTN="gcs-pool"
LISTEN_INNER=":8090"          # 容器内服务监听端口（与 docker-compose.yml 右值一致）
HOST_PORT="11223"             # 默认宿主机端口，自动从 compose 覆盖

# 从 compose 提取宿主机端口（第一个 ports 映射的左侧）
if [ -f "$COMPOSE" ]; then
  P=$(grep -A5 'ports:' "$COMPOSE" | grep -oE '"[0-9]+:' | head -1 | tr -d '":')
  [ -n "$P" ] && HOST_PORT="$P"
fi

gen_config() {
  local pass
  pass=$(openssl rand -hex 16)
  cat > "$CONFIG" <<EOF
{
  "listen": "$LISTEN_INNER",
  "default_bucket": "",
  "max_size": 1024,
  "retry": 3,
  "admin_password": "$pass",
  "max_concurrent": 1024,
  "request_timeout": 1800,
  "retry_429_base": 1,
  "retry_429_max": 5,
  "signed_url_ttl": 604800,
  "ttl_days": 7,
  "health_check_interval": 90,
  "keys_scan_interval": 60,
  "api_keys": [],
  "accounts": []
}
EOF
  chmod 600 "$CONFIG"
  echo "$pass"
}

cmd_init() {
  mkdir -p keys
  if [ -f "$CONFIG" ]; then
    echo "⚠  config.json 已存在，跳过生成（不覆盖现有配置）"
    echo "    当前密码: $(grep -o '"admin_password": "[^"]*"' "$CONFIG" | head -1 | cut -d'"' -f4)"
  else
    local pass
    pass=$(gen_config)
    echo "=================================================="
    echo "  config.json 已生成（listen=$LISTEN_INNER）"
    echo ""
    echo "  管理台密码: $pass"
    echo "  ↑ 只显示这一次，请立即保存"
    echo "=================================================="
  fi
  echo "👉 下一步：把 SA key 文件放到 keys/ 目录（60s 内自动注册），或在管理台「添加号」"
  cmd_start
  echo "🎉 完成！浏览器访问: http://<服务器IP>:$HOST_PORT/"
}

cmd_start() {
  docker compose up -d --build
  sleep 3
  cmd_status
}

cmd_restart() {
  docker compose up -d --build --force-recreate
  sleep 3
  cmd_status
}

cmd_status() {
  echo "=== 容器 ==="
  docker ps --filter name="$CTN" --format "{{.Names}}  {{.Status}}  {{.Ports}}"
  echo "=== healthz (localhost:$HOST_PORT) ==="
  curl -s -m 5 -o /dev/null -w "HTTP %{http_code}\n" "http://localhost:$HOST_PORT/healthz" || echo "无法连接（服务未启动？）"
  echo "=== 最近日志 ==="
  docker logs --tail 5 "$CTN" 2>&1 || true
}

cmd_logs() { docker logs -f "$CTN"; }
cmd_down() { docker compose down; }

case "${1:-}" in
  init)   cmd_init ;;
  start)  cmd_start ;;
  restart) cmd_restart ;;
  status) cmd_status ;;
  logs)   cmd_logs ;;
  down)   cmd_down ;;
  *) echo "用法: $0 {init|start|restart|status|logs|down}"; exit 1 ;;
esac
