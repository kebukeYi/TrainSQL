# TrainSQL
Golang 实现支持 MVCC 特性 的 SQL 数据库, 其中存储引擎为 BitCast 模型的 kv 引擎;

## 支持的 SQL 语法

### 1. Create/Drop Table
```sql
CREATE TABLE table_name (
    [ column_name data_type [index] [ column_constraint [...] ] ]
    [, ... ]
   );

   where data_type is:
    - BOOLEAN(BOOL): true | false
    - FLOAT(DOUBLE)
    - INTEGER(INT)
    - STRING(TEXT, VARCHAR)

   where column_constraint is:
   [ NOT NULL | NULL | DEFAULT expr ]
   
create table t2 (
      a int primary key,
      b integer default 100,
      c float default 1.1,
      d bool default false,
      e boolean default true,
      f text default 'v1',
      g string default 'v2',
      h varchar default 'v3');
   
```
drop table:
```sql
DROP TABLE table_name;

drop table t2;
```

### 2. Insert Into
```sql
INSERT INTO table_name
[ ( column_name [, ...] ) ]
values ( expr [, ...] );

insert into t2 values (1, 2, 3.0, true, false, 'v1', 'v2', 'v3');
```

### 3. Select
```sql
SELECT [* | col_name | function [ [ AS ] output_name [, ...] ]]
FROM from_item
[GROUP BY col_name]
[ORDER BY col_name [asc | desc] [, ...]]
[LIMIT count]
[OFFSET count]

select * from t2;
select * from t2 where a = 1;
select * from t2 where a > 1 order by a desc;
select * from t2 where a > 1 group by a;
select * from t2 where a > 1 group by a order by a desc;
select count(a) as total, max(b), min(a), sum(c), avg(c) from t2;
select count(a) from t2 group by a order by a desc limit 1 offset 2;
select * from haj1 join haj2 on a = b join haj3 on a = c;
select * from haj1 right join haj2 on a = b join haj3 on a = c;
```

where `function` is: count(col_name), min(col_name), max(col_name), sum(col_name), avg(col_name);

where `from_item` is:
* table_name
* table_name `join_type` table_name [`ON` predicate]

where `join_type` is:
* cross join
* join(inner join)
* left join
* right join

where `on predicate` is:
* column_name = column_name

### 4. Update
```sql
UPDATE table_name
SET column_name = expr [, ...]
[WHERE condition];

update t2 set a = 10 where a < 11;
update t2 set b = 70 where a > 11;
update t2 set c = 80 where a = 11;
```

### 5. Delete
```sql
DELETE FROM table_name
[WHERE condition];

delete from t2 where a < 11;
delete from t2 where a > 11;
delete from t2 where a = 11;
```

### 5. Show Table
```sql
SHOW TABLES;
     
show tables;
showTableNames: t1,t2,t3,t4,test4,test5,test6;
```

```sql
SHOW TABLE `table_name`;
     
show table t2;
COLUMNS: { 
    a Integer PRIMARY KEY
    b Integer DEFAULT 100
    c Float DEFAULT 1.1
    d UNKNOWN DEFAULT false
    e UNKNOWN DEFAULT true
    f String DEFAULT v1
    g String DEFAULT v2
    h String DEFAULT v3
} 
```

### 6. Transaction

```
BEGIN;
TRANSACTION 136 BEGIN

COMMIT;
TRANSACTION 136 COMMIT

ROLLBACK;
TRANSACTION 136 ROLLBACK
```

## 7. Explain
```
explain sql;

explain select * from haj1 join haj2 on a = b join haj3 on a = c;
           SQL PLAN           
------------------------------
 Hash Join (a = c)
  ->   Hash Join (a = b)
     ->  Seq Scan on  haj1
     ->  Seq Scan on  haj2
  ->  Seq Scan on  haj3
```
