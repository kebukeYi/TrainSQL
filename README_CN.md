<div align="center">
<strong>
<samp>

[English](https://github.com/kebukeYi/TrainSQL/blob/main/README.md) · [简体中文](https://github.com/kebukeYi/TrainSQL/blob/main/README_CN.md)

</samp>
</strong>
</div>


# TrainSQL
[![Go](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache_2.0-green)](https://opensource.org/licenses/Apache-2.0)

A simple SQL database implemented in Golang, supporting transactions and MVCC (Multi-Version Concurrency Control) features. The storage layer uses a KV storage engine based on the BitCask model.

# 🚀 快速开始

## 📋 前置要求

- Go 1.23 或更高版本
- 终端/命令行工具

---

## ⚡ 快速体验

### 1️⃣ 启动服务端

进入项目 bin 目录并启动 SQL 服务器：

```bash
cd bin
go run server.go model.go -d /path/to/data -p 8888
```

**参数说明**：
- `-d`: 数据存储路径 (例如: `./data` 或 `/tmp/trainsql`)
- `-p`: 服务端口号 (默认: 8888)

---

### 2️⃣ 连接客户端

在新的终端窗口中启动客户端：

```bash
cd bin
go run client.go model.go -s 127.0.0.1:8888
```

**参数说明**：
- `-s`: 服务器地址 (格式: `IP:端口`)

---

### 3️⃣ 执行 SQL 命令

连接成功后，可以执行以下示例命令：

#### 📝 创建表

```sql
-- 创建三个测试表
CREATE TABLE haj1 (a INT PRIMARY KEY);
CREATE TABLE haj2 (b INT PRIMARY KEY);
CREATE TABLE haj3 (c INT PRIMARY KEY);
```

#### 📥 插入数据

```sql
-- 向表中插入测试数据
INSERT INTO haj1 VALUES (1), (2), (3);
INSERT INTO haj2 VALUES (2), (3), (4);
INSERT INTO haj3 VALUES (3), (1), (9);
```

#### 🔍 查询数据

```sql
-- 多表 JOIN 查询
SELECT * FROM haj1 
  JOIN haj2 ON a = b 
  JOIN haj3 ON a = c;
```

**预期结果**：
```
a | b | c
--+---+--
3 | 3 | 3
```

#### 📊 查看执行计划

```sql
-- 查看 SQL 执行计划 (用于性能分析)
EXPLAIN SELECT * FROM haj1 
  JOIN haj2 ON a = b 
  JOIN haj3 ON a = c;
```

---

## 💡 更多示例

### 事务操作

```sql
-- 手动事务控制
BEGIN;
INSERT INTO haj1 VALUES (10);
COMMIT;

-- 事务回滚
BEGIN;
DELETE FROM haj1 WHERE a = 10;
ROLLBACK;
```

### 索引查询

```sql
-- 创建带索引的表
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL,
  age INT INDEX
);

-- 利用索引查询
SELECT * FROM users WHERE age = 25;
```

### 聚合查询

```sql
-- GROUP BY 和聚合函数
SELECT age, COUNT(id), AVG(age) 
FROM users 
GROUP BY age 
HAVING COUNT(id) > 1;
```

---

## 🛠️ 故障排查

| 问题 | 解决方案 |
|------|----------|
| 端口被占用 | 修改 `-p` 参数使用其他端口 |
| 连接失败 | 检查服务端是否启动，防火墙设置 |
| 数据丢失 | 确保 `-d` 路径有写权限 |

---

## 📚 支持的 SQL 语法

- ✅ DDL: `CREATE TABLE`, `DROP TABLE`
- ✅ DML: `INSERT`, `UPDATE`, `DELETE`, `SELECT`
- ✅ JOIN: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `CROSS JOIN`
- ✅ 聚合: `COUNT`, `SUM`, `AVG`, `MAX`, `MIN`
- ✅ 子句: `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`
- ✅ 事务: `BEGIN`, `COMMIT`, `ROLLBACK`
- ✅ 其他: `EXPLAIN`, `SHOW TABLE`, `SHOW DATABASE`

---

## ⚠️ 已知限制

> 以下是当前版本的已知限制和待实现功能

### 语法限制

| 限制项 | 说明 | 示例 |
|:------|:-----|:----|
| 表名限定符 | JOIN ON 条件不支持 `表名.列名` 格式 | ❌ `ON users.id = orders.user_id` |
| 常量比较 | ON 条件不支持与常量比较 | ❌ `ON users.id = 3` |
| 比较运算符 | WHERE 仅支持 `=`, `>`, `<`，不支持 `>=`, `<=`, `!=` | ❌ `WHERE id >= 11` |

### 数据类型限制

| 限制项 | 说明 |
|:------|:-----|
| STRING 长度 | 未对 STRING/VARCHAR 类型设置最大长度限制 |

### 性能限制

| 限制项 | 说明 |
|:------|:-----|
| 范围查询 | 使用 `>` 或 `<` 进行范围查询时，退化为全表扫描，无法利用索引 |
| 锁粒度 | 仅支持事务级别的并发控制，暂不支持更细粒度的行级锁 |

### 事务限制

| 限制项 | 说明 |
|:------|:-----|
| 崩溃恢复 | 当事务未提交时数据库异常关闭，重启后未提交事务的中间状态数据未能自动清理 |

---

## 🗺️ Roadmap

- [ ] 支持表名限定符 (`table.column`)
- [ ] 支持 `>=`, `<=`, `!=`, `<>` 比较运算符
- [ ] 支持  `OR` 逻辑运算符
- [ ] 支持  `IN`,`LIKE` 运算符
- [ ] 实现范围查询的索引优化
- [ ] 优化器,查询树效率分析
- [ ] 添加 varChar 类型长度约束
- [ ] 使用B+Tree为存储引擎
- [ ] 实现行级锁

