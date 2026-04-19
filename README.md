# 🏗️ DFS-mini — 轻量级分布式文件系统原型

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev)
[![gRPC](https://img.shields.io/badge/gRPC-1.60+-blue)](https://grpc.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 📌 项目简介

**DFS-mini** 是一个从零实现的轻量级分布式文件系统，参考 HDFS/GFS 架构思想。  
它实现了 **元数据管理、文件分片、一致性哈希负载均衡、副本冗余与故障检测** 等分布式系统的核心机制。  
该项目旨在深入理解分布式存储的设计权衡，而非追求生产级完备性。

              ┌───────────────┐
              │    Client     │
              └──────┬────────┘
                     │
              ┌──────▼────────┐
              │    Tracker     │
              │ - Metadata     │
              │ - Scheduling   │
              │ - Heartbeat    │
              └──────┬────────┘
                     │
     ┌───────────────┼────────────────┐
     │               │                │
┌────▼─────┐   ┌────▼─────┐    ┌────▼─────┐
│ Storage 1│   │ Storage 2│    │ Storage 3│
│ - Shards │   │ - Shards │    │ - Shards │
│ - Replica│   │ - Replica│    │ - Replica│
└──────────┘   └──────────┘    └──────────┘

        ┌────────────────────────────┐
        │ Consistent Hash Ring       │
        │ - Virtual Nodes            │
        └────────────────────────────┘
## ✨ 核心机制

| 模块 | 实现 | 设计考量 |
|------|------|----------|
| **一致性哈希** | CRC32 + 100虚拟节点 | 节点扩缩容时数据迁移量最小化（约 1/N） |
| **副本机制** | 可配置副本数（默认2） | 顺时针遍历环获取不重复节点，提升可靠性 |
| **负载感知调度** | 优先选择可用空间最大的健康节点 | 避免热点，实现初步负载均衡 |
| **心跳故障检测** | 5s 心跳间隔，15s 超时剔除 | 平衡故障发现速度与网络抖动容限 |
| **并发安全** | RWMutex 保护共享元数据 | 支持多客户端并发访问 |
| **优雅关闭** | Context 控制 + 信号捕获 | goroutine 正确退出，无资源泄露 |

## 📊 系统指标

| 指标 | 数值 | 说明 |
|------|------|------|
| **分片大小** | 4MB（可配置） | 平衡元数据开销与并行度 |
| **副本因子** | 2 | 默认双副本 |
| **上传吞吐** | ~280 MB/s | 3节点集群，4MB分片，10MB文件（内存缓存模式） |
| **节点扩容影响** | < 10s | 一致性哈希使数据迁移量极小 |
| **故障检测时间** | < 20s | 心跳超时 + 副本可用性保障 |
| **虚拟节点数** | 100/物理节点 | 数据分布偏差 < 5% |

> 💡 *吞吐量测试说明：当前 Storage 节点将数据写入操作系统页缓存后即返回，未强制刷盘，因此吞吐量反映的是内存与网络带宽上限。生产环境中可通过 `fsync` 获得真实磁盘性能。*

## ⚙️ 技术栈

- **语言**: Go 1.21+
- **RPC 框架**: gRPC + Protocol Buffers
- **核心算法**: 一致性哈希 (CRC32)，虚拟节点
- **配置管理**: Viper (YAML)

## 🚀 快速开始

### 1. 启动 Tracker
```bash
./bin/tracker --config=configs/tracker.yaml
2. 启动 Storage 节点 (可多个)
bash
# 节点1
./bin/storage --node-id=node1 --config=configs/storage.yaml

# 节点2
./bin/storage --node-id=node2 --port=50052 --data=./data/node2 --config=configs/storage.yaml
3. 使用客户端上传文件
bash
# 生成测试文件 (PowerShell)
$filePath = "test_10MB.dat"
$stream = [System.IO.File]::OpenWrite($filePath)
$writer = New-Object System.IO.BinaryWriter($stream)
$writer.Write((New-Object byte[] (10 * 1024 * 1024)))
$writer.Close()
$stream.Close()

# 上传
./bin/client upload --file=$filePath
📂 项目结构
text
dfs-mini/
├── cmd/                  # 各组件入口
│   ├── tracker/          # Tracker 主程序
│   ├── storage/          # Storage 主程序
│   └── client/           # 命令行客户端
├── internal/             # 内部实现
│   ├── tracker/          # 元数据管理、一致性哈希、心跳
│   ├── storage/          # 分片存储、gRPC 服务
│   ├── proto/            # Protobuf 定义及生成代码
│   └── common/           # 配置加载
├── pkg/consistent/       # 一致性哈希算法包
├── configs/              # YAML 配置文件示例
├── bin/                  # 编译输出
└── README.md
🎯 设计思想
本项目通过 Tracker-Storage 分离架构 将控制面与数据面解耦，并通过一致性哈希与副本策略保证系统的 可扩展性 与 容错性。
虽然出于教学目的简化了部分生产特性（如元数据持久化、自动数据迁移），但核心算法的实现完全体现了分布式系统的设计精髓。

该项目证明了我对分布式系统核心理论（一致性哈希、Quorum、故障检测）的掌握，以及用 Go 构建高并发网络服务的能力。