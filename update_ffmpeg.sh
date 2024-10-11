#!/bin/bash

# 定义变量
FFMPEG_VERSION="ffmpeg-n7.1-latest-linux64-gpl-shared-7.1.tar.xz"
DOWNLOAD_URL="https://ghp.ci/https://github.com/Uranite/FFmpeg-Builds/releases/download/latest/$FFMPEG_VERSION"
INSTALL_DIR="/home/tosaki/ffmpeg-psy"

# 创建临时目录用于下载
TEMP_DIR=$(mktemp -d)

# 下载 ffmpeg 构建
echo "正在下载 $FFMPEG_VERSION ..."
wget -O "$TEMP_DIR/$FFMPEG_VERSION" "$DOWNLOAD_URL"

# 检查下载是否成功
if [ $? -ne 0 ]; then
  echo "下载失败，请检查 URL 或网络连接。"
  exit 1
fi

# 如果目标安装目录不存在，则创建
if [ ! -d "$INSTALL_DIR" ]; then
  echo "创建安装目录 $INSTALL_DIR ..."
  mkdir -p "$INSTALL_DIR"
fi

# 解压缩下载的文件
echo "解压缩 $FFMPEG_VERSION 到 $INSTALL_DIR ..."
tar -xJf "$TEMP_DIR/$FFMPEG_VERSION" -C "$INSTALL_DIR" --strip-components=1

# 检查解压缩是否成功
if [ $? -ne 0 ]; then
  echo "解压缩失败，请检查压缩文件。"
  exit 1
fi

# 清理临时目录
rm -rf "$TEMP_DIR"

# 添加到 PATH（可选）
echo "将 ffmpeg 添加到 PATH ..."
export PATH="$INSTALL_DIR/bin:$PATH"
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$LD_LIBRARY_PATH"

# 检查 ffmpeg 是否已安装成功
echo "ffmpeg 版本信息："
ffmpeg -version

echo "ffmpeg 安装完成。"