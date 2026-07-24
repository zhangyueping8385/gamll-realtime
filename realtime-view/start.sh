#!/bin/bash

# 实时数据可视化模块启动脚本

echo "正在启动实时数据可视化模块..."

# 检查Java环境
if ! command -v java &> /dev/null; then
    echo "错误：未找到Java环境，请先安装Java 8或更高版本"
    exit 1
fi

# 检查Maven环境
if ! command -v mvn &> /dev/null; then
    echo "错误：未找到Maven环境，请先安装Maven"
    exit 1
fi

# 编译项目
echo "正在编译项目..."
cd /Users/xiaozhang/Code/JavaCode/实时数仓4.0/gmall-realtime/realtime-view
mvn clean compile

if [ $? -ne 0 ]; then
    echo "错误：项目编译失败"
    exit 1
fi

# 启动应用
echo "正在启动应用..."
mvn spring-boot:run

if [ $? -ne 0 ]; then
    echo "错误：应用启动失败"
    exit 1
fi