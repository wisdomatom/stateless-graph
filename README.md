# Stateless Graph (无状态图数据协议与计算引擎)

[![Go Version](https://img.shields.io/badge/Go-%3E%3D1.22-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE.txt)
[![Status](https://img.shields.io/badge/Status-Active-brightgreen.svg)]()

**Stateless Graph** 是一个极简、轻量、高纯度的**开源图数据协议与无状态图计算引擎**。

它摒弃了传统图数据库沉重的有状态单体与复杂的 Raft 状态机运维负担，采用现代云原生**“存算分离”**架构，底层由分布式强一致事务存储（**TiKV**）或单机轻量引擎（**BadgerDB**）驱动，专注于提供标准图遍历、DQL 执行求值、倒排索引与 MVCC 图模型。

---

## 核心特性

- 🚀 **存算分离与纯无状态 (Stateless Architecture)**：
  计算节点不绑定本地状态，节点无状态弹性扩缩容，彻底解决图数据库分布式再平衡（Rebalance）与集群分裂难题。
- ⚡ **TiKV 原生分布式事务驱动 (TiKV-Native Backing)**：
  依托 TiKV 分布式 Percolator 事务模型与 Multi-Raft 存储底座，原生支持 PB 级图数据、ACID 强一致性及毫秒级图检索。
- 📦 **零开销进程内嵌入模式 (Embedded In-Process Engine)**：
  支持直接作为 Go 依赖包导入项目，内部通过虚拟管道内存直连，**0 外部端口监听，0 网络开销**，可在 30ms 内完成热启动、事务写入与多跳图回查。
- 🔍 **图数据协议与丰富计算 (DQL & Graph Protocol)**：
  支持完整的属性图（Property Graph）模型，包含深度多跳图遍历、条件级联过滤、递归最短路径、Facets 边属性及倒排全文检索。
- 🧩 **自由网关与解耦架构**：
  引擎专注于图协议与底层计算，上层可自由搭载独立网关（如 GraphQL 网关 `dgraph-graphql`、REST API 或自建微服务），拒绝臃肿耦合。

---

## 架构概览

```
               +--------------------------------------+
               |    Upper Gateway / Client Apps       |
               | (dgraph-graphql / Custom API / CLI)  |
               +--------------------------------------+
                                  │ (Go In-Process Direct Call / gRPC)
                                  ▼
      ==========================================================
      |               Stateless Graph Core Engine              |
      |  ┌─────────────┐   ┌─────────────┐   ┌───────────────┐ |
      |  | DQL Parser  |   | Graph Query |   | Schema/Types  | |
      |  └─────────────┘   └─────────────┘   └───────────────┘ |
      |  ┌─────────────┐   ┌─────────────┐   ┌───────────────┐ |
      |  | MVCC Posting|   |   Tokenizer |   | Graph Algo    | |
      |  └─────────────┘   └─────────────┘   └───────────────┘ |
      ==========================================================
                                  │
                                  ▼
               +--------------------------------------+
               |        Storage Engine Layer          |
               |   [TiKV Distributed KV] / [BadgerDB]  |
               +--------------------------------------+
```

---

## 快速上手 (Quick Start)

### 1. 作为 Go 模块依赖引入

```bash
go get github.com/wisdomatom/stateless-graph
```

### 2. 嵌入式模式 (In-Process Mode)

无需额外部署独立图数据库进程，直接在 Go 代码中以内置库形式运行：

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/wisdomatom/stateless-graph/embeding"
)

func main() {
	ctx := context.Background()

	// 1. 初始化无状态图引擎（直连 TiKV 集群）
	ed, err := embeding.NewEmbedDgraph(embeding.Options{
		TiKVAddrs: []string{"127.0.0.1:2379"},
		TmpDir:    "/tmp/stateless_graph",
	})
	if err != nil {
		log.Fatalf("Failed to initialize graph engine: %v", err)
	}
	defer ed.Close()

	// 2. 写入图节点与属性 (Mutation)
	nquads := `
		_:user <name> "Alice" .
		_:user <dgraph.type> "User" .
	`
	assigned, err := ed.Mutate(ctx, []byte(nquads), true)
	if err != nil {
		log.Fatalf("Mutation failed: %v", err)
	}
	fmt.Printf("Created Node UID: %s\n", assigned.Uids["user"])

	// 3. 执行 DQL 图查询 (Query)
	query := fmt.Sprintf(`{
		user(func: uid(%s)) {
			uid
			name
		}
	}`, assigned.Uids["user"])

	resp, err := ed.Query(ctx, query)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Query Response: %s\n", string(resp.Json))
}
```

---

## 独立节点模式 (Alpha CLI)

`stateless-graph` 同样支持编译为独立的无状态计算节点服务：

```bash
# 编译二进制
go build -o bin/stateless-graph ./dgraph

# 启动无状态图服务（连接外部 TiKV）
./bin/stateless-graph alpha \
  --my=127.0.0.1:7080 \
  --tikv="127.0.0.1:2379" \
  --postings=/tmp/stateless_p \
  --wal=/tmp/stateless_w
```

---

## 模块结构

```text
stateless-graph/
├── embeding/      # 零端口全内存嵌入式引擎入口
├── dql/           # DQL 语法解析器与抽象语法树 (AST)
├── query/         # 图遍历、执行计划与求值引擎
├── posting/       # 倒排列表与 MVCC 多版本并发控制
├── schema/        # 图 Schema 定义与元数据校验
├── types/         # 基础标量类型与 Facets 边属性比较器
├── tok/           # 全文检索、精确匹配与向量分词器
├── algo/          # 高性能 UID 集合交并差算法
├── x/             # 统一存储适配层 (TiKV / BadgerDB)
└── protos/        # 图通信与标准序列化协议定义
```

---

## 开源协议

本项目遵循 [Apache License 2.0](LICENSE.txt)。
