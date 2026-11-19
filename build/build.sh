#!/usr/bin/env bash
#
# 打包为单文件二进制（PHP 代码 phar + 静态 PHP 运行时 micro.sfx）
#
# 依赖：
#   1. composer
#   2. static-php-cli 的 spc（https://static-php.dev）
#   3. 已用 spc 构建好的 micro.sfx（需含 relay 用到的扩展）
#
# 用法：
#   ./build/build.sh              # 同时生成 dist/relay 和 dist/consumer
#   ./build/build.sh relay        # 只打生产端
#   ./build/build.sh consumer     # 只打消费端
#   SPC=/path/to/spc ./build/build.sh
#
# 没有 micro.sfx 时仍会生成 phar，可直接 `php dist/relay.phar start` 运行。
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="$ROOT/dist"
SPC="${SPC:-spc}"
MICRO_SFX="${MICRO_SFX:-$ROOT/build/micro.sfx}"
TARGET="${1:-all}"

echo "==> 1/6 清理与安装生产依赖"
rm -rf "$DIST"
mkdir -p "$DIST"
composer install --no-dev --classmap-authoritative --no-interaction --working-dir="$ROOT" --quiet

# 准备一份公共 stage（src/config/vendor），两个入口共用
STAGE_BASE="$DIST/stage-base"
mkdir -p "$STAGE_BASE"
cp -r "$ROOT/src" "$ROOT/config" "$ROOT/vendor" "$STAGE_BASE/"
mkdir -p "$STAGE_BASE/var"
find "$STAGE_BASE" -name '.env' -delete
find "$STAGE_BASE/var" -type f -delete

echo "==> 2/6 生成 phar"
build_one() {
  local name="$1"      # relay | consumer
  local entry="$2"     # relay.php | consumer.php
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
  "$SPC" micro:combine "$phar" \
    --with-micro="$MICRO_SFX" \
    --output="$out" \
    -I "memory_limit=512M" \
    -I "display_errors=stderr" \
    -I "log_errors=1" \
    -I "error_reporting=E_ALL & ~E_DEPRECATED" >/dev/null
  chmod +x "$out"
  echo "    -> $out ($(du -h "$out" | cut -f1))"
}

case "$TARGET" in
  relay)    build_one relay relay.php ;;
  consumer) build_one consumer consumer.php ;;
  all)      build_one relay relay.php; build_one consumer consumer.php ;;
  *)        echo "未知目标：$TARGET（可选 relay / consumer / all）"; exit 1 ;;
esac

rm -rf "$STAGE_BASE"

echo
echo "==> 完成，产物在 $DIST"
if [[ ! -f "$MICRO_SFX" ]]; then
  cat <<EOF

要生成单文件二进制，先在能编译的机器上构建 micro.sfx：

  spc build:php "sockets,bcmath,pdo_mysql,phar,pcntl,posix,redis" --build-micro
  cp buildroot/bin/micro.sfx build/micro.sfx

消费端若使用 mysql sink，同样需要 pdo_mysql；使用 redis 点位/队列需要 redis 扩展。
EOF
fi
