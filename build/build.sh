#!/usr/bin/env bash
#
# 打包为单文件二进制（PHP 代码 phar + 静态 PHP 运行时 micro.sfx）
#
# 依赖：
#   1. composer（仅用于生成 autoload，项目本身零第三方包）
#   2. micro.sfx 运行时：本地没有时会自动从 workerman 下载
#
# 不需要 spc：合成逻辑由 build/micro-combine.php 用纯 PHP 实现，
# 格式与 spc 的 micro:combine 完全一致。
#
# 用法：
#   ./build/build.sh                    # 三个入口全打
#   ./build/build.sh relay              # 只打生产端
#   ./build/build.sh consumer           # 只打消费端
#   ./build/build.sh binlog2redis       # 只打单体服务
#   MICRO_SFX=/path/to/micro.sfx ./build/build.sh   # 用别的运行时
#
# 跨平台注意：下载的 micro.sfx 是 Linux x86-64 可执行文件。
# 在 Linux 上打包可直接运行；在 Windows/macOS 上打包得到的是 Linux 产物
# （可用于交叉部署），本机无法执行。
#
# 没有 micro.sfx 且下载失败时仍会生成 phar，可直接 `php dist/relay.phar start`。
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="$ROOT/dist"
MICRO_SFX="${MICRO_SFX:-$ROOT/build/micro.sfx}"
TARGET="${1:-all}"
PHP_VERSION="${PHP_VERSION:-8.4}"
MICRO_URL="${MICRO_URL:-https://download.workerman.net/php/php${PHP_VERSION}.micro.sfx}"

# 解析目标平台：显式指定优先，否则按当前系统推断
TARGET_OS="${TARGET_OS:-$(uname -s 2>/dev/null | tr '[:upper:]' '[:lower:]')}"
TARGET_ARCH="${TARGET_ARCH:-$(uname -m 2>/dev/null)}"

# 注入到二进制里的运行时 ini（可按需增删）
INI_FLAGS=(
  --ini="memory_limit=512M"
  --ini="display_errors=stderr"
  --ini="log_errors=1"
  --ini="error_reporting=E_ALL & ~E_DEPRECATED"
)

echo "==> 1/4 清理与准备依赖"
rm -rf "$DIST"
mkdir -p "$DIST"
composer install --no-dev --classmap-authoritative --no-interaction --working-dir="$ROOT" --quiet

# 自动获取 micro.sfx（本地优先，缺失则从 workerman 下载）
ensure_micro_sfx() {
  if [[ -s "$MICRO_SFX" ]]; then
    echo "    使用已有运行时：$MICRO_SFX ($(du -h "$MICRO_SFX" | cut -f1))"
    return 0
  fi

  echo "    本地无 micro.sfx，尝试下载：$MICRO_URL"
  mkdir -p "$(dirname "$MICRO_SFX")"

  local tmp="$MICRO_SFX.part"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --max-time 300 -o "$tmp" "$MICRO_URL" || true
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "$tmp" "$MICRO_URL" || true
  else
    echo "    没有 curl/wget，跳过下载"
    return 1
  fi

  # 校验：至少 1MB，且是 ELF / Mach-O / PE 之一
  if [[ ! -s "$tmp" ]] || [[ $(wc -c < "$tmp") -lt 1048576 ]]; then
    echo "    下载失败或文件异常，跳过合成"
    rm -f "$tmp"
    return 1
  fi
  if ! head -c 4 "$tmp" | od -An -tx1 | grep -qE '7f 45 4c 46|cf fa ed fe|4d 5a'; then
    echo "    下载内容不是可执行文件，跳过合成"
    rm -f "$tmp"
    return 1
  fi

  mv "$tmp" "$MICRO_SFX"
  chmod +x "$MICRO_SFX" 2>/dev/null || true
  echo "    下载完成：$MICRO_SFX ($(du -h "$MICRO_SFX" | cut -f1))"
  echo "    平台：$(uname -s 2>/dev/null || echo unknown) / $(uname -m 2>/dev/null || echo unknown)"
  echo "    注意：workerman 提供的这些是 Linux x86-64 运行时"
}

HAS_MICRO=1
ensure_micro_sfx || HAS_MICRO=0

# 公共 stage（src/config/vendor），三个入口共用
STAGE_BASE="$DIST/stage-base"
mkdir -p "$STAGE_BASE"
cp -r "$ROOT/src" "$ROOT/config" "$ROOT/vendor" "$STAGE_BASE/"
mkdir -p "$STAGE_BASE/var"
find "$STAGE_BASE" -name '.env' -delete
find "$STAGE_BASE/var" -type f -delete

echo "==> 2/4 生成 phar 并合成二进制"
build_one() {
  local name="$1"      # relay | consumer | binlog2redis
  local entry="$2"     # relay.php | consumer.php | binlog2redis.php
  local stage="$DIST/stage-$name"

  cp -r "$STAGE_BASE" "$stage"
  cp "$ROOT/bin/$name" "$stage/$entry"

  local phar="$DIST/$name.phar"
  php -d phar.readonly=0 "$ROOT/build/make-phar.php" "$stage" "$phar" "$entry"
  rm -rf "$stage"

  if [[ ! -f "$MICRO_SFX" ]]; then
    echo "    [跳过二进制] 未找到 micro.sfx，只生成了 $phar"
    return 0
  fi

  local out="$DIST/$name"
  php "$ROOT/build/micro-combine.php" "$phar" "$out" \
    --micro="$MICRO_SFX" "${INI_FLAGS[@]}" | sed 's/^/    /'
}

echo "==> 3/4 打包入口"
case "$TARGET" in
  relay)         build_one relay relay.php ;;
  consumer)      build_one consumer consumer.php ;;
  binlog2redis)  build_one binlog2redis binlog2redis.php ;;
  all)
    build_one relay relay.php
    build_one consumer consumer.php
    build_one binlog2redis binlog2redis.php
    ;;
  *)
    echo "未知目标：$TARGET（可选 relay / consumer / binlog2redis / all)"
    exit 1
    ;;
esac

rm -rf "$STAGE_BASE"

echo "==> 4/4 完成，产物在 $DIST"
ls -lh "$DIST" | grep -vE '^total|stage' || true

if [[ ! -f "$MICRO_SFX" ]]; then
  cat <<EOF

未找到 micro.sfx，只生成了 phar（可直接用系统 PHP 运行）：
  php dist/relay.phar status

要生成单文件二进制，请准备一个 micro.sfx（含以下扩展）：
  sockets, bcmath, json, phar, pdo_mysql, redis, pcntl, posix

获取方式（任选其一）：
  A. 从 https://github.com/crazywhalecc/static-php-cli/releases 下载预编译版本
  B. 在自己的机器上用 spc 构建一次（只需一次，之后打包不再需要 spc）：
       spc build:php "sockets,bcmath,pdo_mysql,phar,pcntl,posix,redis" --build-micro
       cp buildroot/bin/micro.sfx build/micro.sfx

然后重新执行 ./build/build.sh 即可。
EOF
fi
