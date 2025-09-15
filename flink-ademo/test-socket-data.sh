#!/bin/bash

# 测试数据发送脚本
# 向Socket服务器发送测试数据以验证Flink作业处理

echo "开始向Socket服务器发送测试数据..."
echo "目标: 127.0.0.1:9999"
echo "按Ctrl+C停止"
echo "================================="

# 测试数据数组
test_data=(
    "hello world"
    "apache flink streaming"
    "real time processing"
    "hello flink hello world"
    "streaming data processing"
    "word count example"
    "apache flink rocks"
    "hello streaming world"
)

# 循环发送数据
counter=1
while true; do
    # 随机选择一行测试数据
    data_index=$((RANDOM % ${#test_data[@]}))
    message="${test_data[$data_index]}"
    
    echo "[$counter] 发送: $message"
    echo "$message" | nc 127.0.0.1 9999
    
    # 等待2秒
    sleep 2
    
    counter=$((counter + 1))
done 