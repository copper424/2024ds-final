#!/bin/bash
cd bin/

# 存储后台进程的 PID
pids=()
# pids+=($BASHPID) # $BASHPID 获取当前进程的 PID
# # 启动命令并放入后台
./namenode --secondary_namenode localhost:20242 --restoreFile ./secondary_backup.json &
pids+=("$!") # $! 获取最后一个后台进程的 PID
./secondary_namenode &
pids+=("$!")
./datanode --port 20251 --dataDir datadir1 &                                                                                                                  
pids+=("$!")
./datanode --port 20252 --dataDir datadir2 &
pids+=("$!")
./datanode --port 20253 --dataDir datadir3 &
pids+=("$!")

echo "${pids[@]}"
# 捕获 Ctrl+C 信号
trap 'for pid in "${pids[@]}"; do kill "$pid"; done; exit 1' INT

# 等待所有后台进程结束
wait
cd ..