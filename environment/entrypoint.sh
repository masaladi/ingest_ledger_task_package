#!/bin/bash
# 上一任留下的端口占位进程：作为容器主进程运行（属于现场遗留物，由值班人自行清理）。
# 清理方式与行为约定见 squatter.py 注释：终止信号会使其让出端口并转为保活镜像。
exec python3 /opt/squatter.py
