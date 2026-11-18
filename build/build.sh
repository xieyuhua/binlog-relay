#!/usr/bin/env bash
#
# 打包为单文件二进制（类 webman 的方式：PHP 代码 + 静态 PHP 运行时合成一个可执行文件）
#
# 依赖：
#   1. composer
#   2. static-php-cli 的 spc（https://static-php.dev）
#   3. 已用 spc 构建好的 micro.sfx（需含 sockets/bcmath/pdo_mysql/phar/redis 等扩展）
#
# 用法：
#   ./build/build.sh              # 生成 dist/relay
#   SPC=/path/to/spc ./build/build.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="$ROOT/dist"
STAGE="$DIST/stage"
PHAR="$DIST/relay.phar"
OUT="$DIST/relay"

SPC="${SPC:-spc}"
MICRO_SFX="${MICRO_SFX:-$ROOT/build/micro.sfx}"

echo "==> 1/5 清理"
rm -rf "$DIST"
mkdir -p "$STAGE"

echo "==> 2/5 安装生产依赖"
composer install --no-dev --classmap-authoritative --no-interaction --working-dir="$ROOT"

echo "==> 3/5 收集文件"
cp -r "$ROOT/src" "$STAGE/"
cp -r "$ROOT/config" "$STAGE/"
cp -r "$ROOT/vendor" "$STAGE/"
cp "$ROOT/bin/relay" "$STAGE/relay.php"
mkdir -p "$STAGE/var"

# 打包时不带 .env、点位文件与日志
find "$STAGE" -name '.env' -delete
find "$STAGE/var" -type f -delete

echo "==> 4/5 生成 phar"
php -d phar.readonly=0 "$ROOT/build/make-phar.php" "$STAGE" "$PHAR"

if [[ ! -f "$MICRO_SFX" ]]; then
  cat <<EOF

未找到 micro.sfx：$MICRO_SFX

先生成运行时（任选其一）：

  # A. 用 spc 构建（含 relay 需要的扩展）
  spc build:php "sockets,bcmath,pdo_mysql,phar,redis,pcntl,posix" --build-micro
  cp buildroot/bin/micro.sfx build/micro.sfx

  # B. 从发布页下载预编译的 micro.sfx（注意要包含 sockets 扩展）
  #    https://github.com/crazywhalecc/static-php-cli/releases

phar 已生成：$PHAR
如果需要基于系统 PHP 运行，可直接：
  php $PHAR start
EOF
  exit 0
fi

echo "==> 5/5 合成单文件二进制"
"$SPC" micro:combine "$PHAR" \
  --with-micro="$MICRO_SFX" \
  --output="$OUT" \
  -I "memory_limit=512M" \
  -I "display_errors=stderr" \
  -I "log_errors=1" \
  -I "error_reporting=E_ALL & ~E_DEPRECATED"

chmod +x "$OUT"
echo
echo "完成：$OUT"
echo "运行："
echo "  RELAY_CONFIG=./config/relay.php $OUT start"
