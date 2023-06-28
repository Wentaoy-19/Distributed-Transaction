#!/bin/bash

port=$1 # 定义要杀死的端口号
pid=$(lsof -t -i:$port)

# 终止该进程
if [ ! -z "$pid" ]; then
    echo "Killing process with PID $pid"
    kill $pid
else
    echo "No process is listening on port $port"
fi
