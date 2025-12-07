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

# 🚀 Quick Start

## 📋 Prerequisites

- Go 1.23 or higher
- Terminal/Command line tool

---

## ⚡ Quick Experience

### 1️⃣ Start the Server

Navigate to the project `bin` directory and start the SQL server:

```bash
cd bin
go run server.go model.go -d /path/to/data -p 8888
```

**Parameters**:
- `-d`: Data storage path (e.g., `./data` or `/tmp/trainsql`)
- `-p`: Server port number (default: 8888)

---

### 2️⃣ Connect the Client

Open a new terminal window and start the client:

```bash
cd bin
go run client.go model.go -s 127.0.0.1:8888
```

**Parameters**:
- `-s`: Server address (format: `IP:Port`)

---

### 3️⃣ Execute SQL Commands

After successful connection, you can execute the following example commands:

#### 📝 Create Tables

```sql
-- Create three test tables
CREATE TABLE haj1 (a INT PRIMARY KEY);
CREATE TABLE haj2 (b INT PRIMARY KEY);
CREATE TABLE haj3 (c INT PRIMARY KEY);
```

#### 📥 Insert Data

```sql
-- Insert test data into tables
INSERT INTO haj1 VALUES (1), (2), (3);
INSERT INTO haj2 VALUES (2), (3), (4);
INSERT INTO haj3 VALUES (3), (1), (9);
```

#### 🔍 Query Data

```sql
-- Multi-table JOIN query
SELECT * FROM haj1 
  JOIN haj2 ON a = b 
  JOIN haj3 ON a = c;
```

**Expected Result**:
```
a | b | c
--+---+--
3 | 3 | 3
```

#### 📊 View Execution Plan

```sql
-- View SQL execution plan (for performance analysis)
EXPLAIN SELECT * FROM haj1 
  JOIN haj2 ON a = b 
  JOIN haj3 ON a = c;
```

---

## 💡 More Examples

### Transaction Operations

```sql
-- Manual transaction control
BEGIN;
INSERT INTO haj1 VALUES (10);
COMMIT;

-- Transaction rollback
BEGIN;
DELETE FROM haj1 WHERE a = 10;
ROLLBACK;
```

### Index Query

```sql
-- Create table with index
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL,
  age INT INDEX
);

-- Query using index
SELECT * FROM users WHERE age = 25;
```

### Aggregate Query

```sql
-- GROUP BY and aggregate functions
SELECT age, COUNT(id), AVG(age) 
FROM users 
GROUP BY age 
HAVING COUNT(id) > 1;
```

---

## 🛠️ Troubleshooting

| Issue | Solution |
|-------|----------|
| Port in use | Change `-p` parameter to use another port |
| Connection failed | Check if server is running, firewall settings |
| Data loss | Ensure `-d` path has write permission |

---

## 📚 Supported SQL Syntax

- ✅ DDL: `CREATE TABLE`, `DROP TABLE`
- ✅ DML: `INSERT`, `UPDATE`, `DELETE`, `SELECT`
- ✅ JOIN: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `CROSS JOIN`
- ✅ Aggregation: `COUNT`, `SUM`, `AVG`, `MAX`, `MIN`
- ✅ Clauses: `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`
- ✅ Transaction: `BEGIN`, `COMMIT`, `ROLLBACK`
- ✅ Others: `EXPLAIN`, `SHOW TABLE`, `SHOW DATABASE`

---

## ⚠️ Known Limitations

> The following are known limitations and features to be implemented in the current version

### Syntax Limitations

| Limitation | Description | Example |
|:-----------|:------------|:--------|
| Table qualifier | JOIN ON conditions do not support `table.column` format | ❌ `ON users.id = orders.user_id` |
| Constant comparison | ON conditions do not support comparison with constants | ❌ `ON users.id = 3` |
| Comparison operators | WHERE only supports `=`, `>`, `<`, not `>=`, `<=`, `!=` | ❌ `WHERE id >= 11` |

### Data Type Limitations

| Limitation | Description |
|:-----------|:------------|
| STRING length | No maximum length limit for STRING/VARCHAR types |

### Performance Limitations

| Limitation | Description |
|:-----------|:------------|
| Range query | Range queries using `>` or `<` degrade to full table scan, cannot utilize index |
| Lock granularity | Only supports transaction-level concurrency control, no fine-grained row-level locks |

### Transaction Limitations

| Limitation | Description |
|:-----------|:------------|
| Crash recovery | When uncommitted transactions exist during abnormal database shutdown, intermediate state data of uncommitted transactions is not automatically cleaned up after restart |

---

## 🗺️ Roadmap

- [ ] Support table qualifiers (`table.column`)
- [ ] Support `>=`, `<=`, `!=`, `<>` comparison operators
- [ ] Support `OR` logical operator
- [ ] Support `IN`, `LIKE` operators
- [ ] Implement index optimization for range queries
- [ ] Optimizer, query tree efficiency analysis
- [ ] Add VARCHAR type length constraints
- [ ] Use B+Tree as storage engine
- [ ] Implement row-level locks

