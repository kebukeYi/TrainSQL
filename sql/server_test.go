package sql

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"github.com/kebukeYi/TrainSQL/storage"
	"strings"
	"testing"
)

var dirPath = "/usr/golanddata/trainsql/server"

func testCreateTable(t *testing.T, session *Session) {
	resultSet := session.Execute(`create table t1 (
    									a int primary key, 
    									b text default 'vv',
    									c integer default 100);`)
	fmt.Println(resultSet.ToString())
	resultSet = session.Execute(`create table t2 (
    									a int primary key, 
										b integer default 100,
										c float default 1.1,
										d bool default false,
										e boolean default true,
										f text default 'v1',
										g string default 'v2',
										h varchar default 'v3');`)
	fmt.Println(resultSet.ToString())
	resultSet = session.Execute(`create table t3 (
  										 a int primary key, 
  										 b int default 12 null,
  										 c integer default NULL,
  										 d float not null);`)
	fmt.Println(resultSet.ToString())
	resultSet = session.Execute(`create table t4 (
  										 a bool primary key, 
  										 b int default 12 ,
  										 d bool default true);`)
	fmt.Println(resultSet.ToString())

}
func testInsertTable(t *testing.T, session *Session) {
	// a |b  |c
	//--+---+----
	//1 |vv |100
	//2 |a  |2
	//3 |b  |100
	resultSet := session.Execute("insert into t1 (a) values (1);")
	fmt.Println(resultSet.ToString())
	resultSet = session.Execute("insert into t1 values (2, 'a', 2);")
	fmt.Println(resultSet.ToString())
	resultSet = session.Execute("insert into t1(b,a) values ('b', 3);")
	fmt.Println(resultSet.ToString())

	//a |b   |c   |d     |e    |f  |g  |h
	//--+----+----+------+-----+---+---+---
	//1 |100 |1.1 |false |true |v1 |v2 |v3
	resultSet = session.Execute("insert into t2 (a) values (1);")
	fmt.Println(resultSet.ToString())

	//a |b  |c    |d
	//--+---+-----+----
	//1 |12 |null |1.1
	resultSet = session.Execute("insert into t3 (a, d) values (1, 1.1);")
	fmt.Println(resultSet.ToString())

	//a    |b  |d
	//-----+---+-----
	//true |12 |true
	resultSet = session.Execute("insert into t4 (a) values (true);")
	fmt.Println(resultSet.ToString())

}
func testScanTable(t *testing.T, session *Session, tableName string) {
	sql := fmt.Sprintf("select * from %s;", tableName)
	resultSet := session.Execute(sql)
	fmt.Println(resultSet.ToString())
}
func testUpdate(t *testing.T, session *Session) {
	resultSet := session.Execute("insert into t2 values (11, 1, 1.1, true, true, 'v1', 'v2', 'v3');")
	resultSet = session.Execute("insert into t2 values (22, 2, 2.2, false, false, 'v4', 'v5', 'v6');")
	resultSet = session.Execute("insert into t2 values (33, 3, 3.3, true, false, 'v7', 'v8', 'v9');")
	resultSet = session.Execute("insert into t2 values (44, 4, 4.4, false, true, 'v10', 'v11', 'v12');")
	//a  |b   |c   |d     |e     |f   |g   |h
	//--+----+----+------+------+----+----+----
	//11 |1   |1.1 |true  |true  |v1  |v2  |v3
	//22 |2   |2.2 |false |false |v4  |v5  |v6
	//33 |3   |3.3 |true  |false |v7  |v8  |v9
	//44 |4   |4.4 |false |true  |v10 |v11 |v12

	//a  |b   |c   |d     |e     |f   |g   |h
	//---+----+----+------+------+----+----+----
	//1  |100 |1.1 |false |true  |v1  |v2  |v3
	//11 |1   |1.1 |true  |true  |v1  |v2  |v3
	//22 |70  |2.2 |false |false |v4  |v5  |v6
	//33 |70  |3.3 |true  |false |v7  |v8  |v9
	//44 |70  |4.4 |false |true  |v10 |v11 |v12
	//(5 rows)
	resultSet = session.Execute("update t2 set b = 70 where a > 11;")
	fmt.Println(resultSet.ToString()) // count = 1

	// a  |b   |c   |d     |e     |f   |g   |h
	//---+----+----+------+------+----+----+----
	//1  |100 |1.1 |false |true  |v1  |v2  |v3
	//11 |1   |1.1 |false |true  |v1  |v2  |v3
	//22 |70  |2.2 |false |false |v4  |v5  |v6
	//33 |70  |3.3 |false |false |v7  |v8  |v9
	//44 |70  |4.4 |false |true  |v10 |v11 |v12
	//(5 rows)
	resultSet = session.Execute("update t2 set d = false where d = true;")
	fmt.Println(resultSet.ToString()) // count = 2

	//           SQL PLAN
	//------------------------------
	//Update on  t2
	//  ->  Seq Scan on  t2 (a > 11)
	resultSet = session.Execute("explain update t2 set b = 70 where a > 11;")
	fmt.Println(resultSet.ToString())
}
func testDelete(t *testing.T, session *Session) {
	resultSet := session.Execute("insert into t2 values (12, 1, 1.1, true, true, 'v1', 'v2', 'v3');")
	resultSet = session.Execute("insert into t2 values (13, 2, 2.2, false, false, 'v4', 'v5', 'v6');")
	resultSet = session.Execute("insert into t2 values (14, 3, 3.3, true, false, 'v7', 'v8', 'v9');")
	resultSet = session.Execute("insert into t2 values (15, 4, 4.4, false, true, 'v10', 'v11', 'v12');")
	fmt.Println(resultSet.ToString())

	// 剩 1 13 14 15
	resultSet = session.Execute("delete from t2 where a = 12;")
	fmt.Println(resultSet.ToString()) // count = 1

	// 剩 14
	resultSet = session.Execute("delete from t2 where d = false;")
	fmt.Println(resultSet.ToString()) // count = 2

	//
	resultSet = session.Execute("explain delete from t2 where d = false;")
	fmt.Println(resultSet.ToString())
}
func testOrderBy(t *testing.T, session *Session) {
	//Scan table t3:
	//a |b  |c  |d
	//--+---+---+-----
	//40 |23 |65 |4.23 |
	//10 |34 |22 |1.22 |
	//30 |56 |22 |2.88 |
	//70 |87 |82 |9.52 |
	//50 |87 |14 |3.28 |
	//20 |87 |57 |6.78 |
	session.Execute("insert into t3 values (10, 34, 22, 1.22);")
	session.Execute("insert into t3 values (40, 23, 65, 4.23);")
	session.Execute("insert into t3 values (30, 56, 22, 2.88);")
	session.Execute("insert into t3 values (20, 87, 57, 6.78);")
	session.Execute("insert into t3 values (50, 87, 14, 3.28);")
	session.Execute("insert into t3 values (70, 87, 82, 9.52);")
	//col2 |a
	//-----+--
	//56   |30
	//87   |70
	//87   |50
	//87   |20
	//(4 rows)
	// b 列升序, a列降序;
	resultSet := session.Execute(`select b as col2, a from t3 order by b, a desc limit 4 offset 2;`)
	// 4行,2列;
	fmt.Println(resultSet.ToString())

	//            SQL PLAN
	//------------------------------
	//Order By (sum_c asc)
	//  ->  Filter (sum_c < 5)
	//     ->  Aggregate (b, sum(c))
	//        ->  Seq Scan on  w1
	resultSet = session.Execute(`explain select b as col2, a from t3 order by b, a desc limit 4 offset 2;`)
	fmt.Println(resultSet.ToString())
}

// ---------------------------------------------------------------------------
func testAgg(t *testing.T, session *Session) {
	session.Execute("create table agg1 (a int primary key, b text, c float);")
	//	//a |b    |c
	//	//--+-----+-----
	//	//1 |aa   |3.1
	//	//2 |cc   |5.3
	//	//3 |null |null
	//	//4 |dd   |4.6
	session.Execute("insert into agg1 values (1, 'aa', 3.1);")
	session.Execute("insert into agg1 values (2, 'cc', 5.3);")
	session.Execute("insert into agg1 values (3, null, NULL);")
	session.Execute("insert into agg1 values (4, 'dd', 4.6);")

	//total |max_b |min_a |sum_c |avg_c
	//------+------+------+------+------------------
	//4     |dd    |1     |13    |4.333333333333333
	resultSet := session.Execute(`select count(a) as total, max(b), min(a), sum(c), avg(c) from agg1;`)
	// 4  dd  1   13.0    13.0/3.0
	fmt.Println(resultSet.ToString())

	//a |b    |c
	//--+-----+-----
	//1 |null |null
	//2 |null |null
	session.Execute("create table agg2 (a int primary key, b text, c float);")
	session.Execute("insert into agg2 values (1, NULL, NULL);")
	session.Execute("insert into agg2 values (2, NULL, NULL);")

	//total |max_b |min_a |sum_c |avg_c
	//------+------+------+------+------
	//2     |null  |1     |null  |null
	resultSet = session.Execute(`select count(a) as total, max(b), min(a), sum(c), avg(c) from agg2;`)
	// 2  null  1   null    null
	fmt.Println(resultSet.ToString())

	//           SQL PLAN
	//------------------------------
	//Aggregate (count(a) as total, max(b), min(a), sum(c), avg(c))
	//  ->  Seq Scan on  agg2
	resultSet = session.Execute("explain select count(a) as total, max(b), min(a), sum(c), avg(c) from agg2;")
	fmt.Println(resultSet.ToString())
}
func testFilter(t *testing.T, session *Session) {
	session.Execute("create table w1 (a int primary key, b text, c float, d bool);")
	session.Execute("insert into w1 values (1, 'aa', 3.1, true);")
	session.Execute("insert into w1 values (2, 'bb', 5.3, true);")
	session.Execute("insert into w1 values (3, null, NULL, false);")
	session.Execute("insert into w1 values (4, null, 4.6, false);")
	session.Execute("insert into w1 values (5, 'bb', 5.8, true);")
	session.Execute("insert into w1 values (6, 'dd', 1.4, false);")
	// where
	//a |b    |c    |d
	//--+-----+-----+------
	//1 |aa   |3.1  |true
	//2 |bb   |5.3  |true
	//3 |null |null |false
	//4 |null |4.6  |false
	//5 |bb   |5.8  |true
	//6 |dd   |1.4  |false
	resultSet := session.Execute("select * from w1 where d < true;")
	// 3行, 4列;
	fmt.Println(resultSet.ToString())
	// having;
	// 注意: sum(c) 默认改为 sum_c 类型;
	// b    |sum_c
	//-----+------
	//dd   |1.4
	//aa   |3.1
	//null |4.6
	resultSet = session.Execute("select b, sum(c) from w1 group by b having sum_c < 5 order by sum_c;")
	// 3行, 2列;
	fmt.Println(resultSet.ToString())

	//
	resultSet = session.Execute("explain select b, sum(c) from w1 group by b having sum_c < 5 order by sum_c;")
	fmt.Println(resultSet.ToString())
}
func testPrimaryKeyScan(t *testing.T, session *Session) {
	session.Execute("create table pk1 (a int primary key, b text index, c float index, d bool);")
	//a |b |c   |d
	//--+--+----+------
	//1 |a |1.1 |true
	//2 |b |2.1 |true
	//3 |a |3.2 |false
	session.Execute("insert into pk1 values (1, 'a', 1.1, true);")
	session.Execute("insert into pk1 values (2, 'b', 2.1, true);")
	session.Execute("insert into pk1 values (3, 'a', 3.2, false);")
	sql := fmt.Sprintf("select * from pk1 where a = 2;")

	//a |b |c   |d
	//--+--+----+-----
	//2 |b |2.1 |true
	resultSet := session.Execute(sql)
	// 1行4列;
	fmt.Println(resultSet.ToString())

	resultSet = session.Execute("explain select * from pk1 where a = 2;")
	fmt.Println(resultSet.ToString())
}
func testIndexScan(t *testing.T, session *Session) {
	session.Execute("create table i1 (a int primary key, b text index, c float index, d bool);")

	session.Execute("insert into i1 values (1, 'a', 1.1, true);")
	session.Execute("insert into i1 values (2, 'b', 2.1, true);")
	session.Execute("insert into i1 values (3, 'a', 3.2, false);")
	session.Execute("insert into i1 values (4, 'c', 1.1, true);")
	session.Execute("insert into i1 values (5, 'd', 2.1, false);")
	session.Execute("delete from i1 where a = 4;")

	// a |b |c   |d
	//--+--+----+-----
	//1 |a |1.1 |true
	resultSet := session.Execute("select * from i1 where c = 1.1;")
	// 1行,4列;
	fmt.Println(resultSet.ToString())

	resultSet = session.Execute("explain select * from i1 where c = 1.1;")
	fmt.Println(resultSet.ToString())
}
func testCrossJoin(t *testing.T, session *Session) {
	session.Execute("create table ac1 (a int primary key);")
	session.Execute("create table ac2 (b int primary key);")
	session.Execute("create table ac3 (c int primary key);")
	session.Execute("insert into ac1 values (1), (2), (3);")
	session.Execute("insert into ac2 values (4), (5), (6);")
	session.Execute("insert into ac3 values (7), (8), (9);")
	//a |b |c
	//--+--+--
	//1 |4 |7
	//1 |4 |8
	//1 |4 |9
	// ... (27 rows)
	resultSet := session.Execute(`select * from ac1 cross join ac2 cross join ac3;`)
	// 27行, 3列;
	fmt.Println(resultSet.ToString())

	resultSet = session.Execute("explain select * from ac1 cross join ac2 cross join ac3;")
	fmt.Println(resultSet.ToString())
}
func testInnerJoin(t *testing.T, session *Session) {
	session.Execute("create table inj1 (a int primary key);")
	session.Execute("create table inj2 (b int primary key);")
	session.Execute("create table inj3 (c int primary key);")
	session.Execute("insert into inj1 values (1), (2), (3);")
	session.Execute("insert into inj2 values (3), (4), (5);")
	session.Execute("insert into inj3 values (3), (8), (9);")
	//b |a |c
	//--+--+--
	//3 |3 |3
	//(1 rows)
	//resultSet := session.Execute(`select * from inj1 right join inj2 on a = b join inj3 on a = c;`)

	//b |a
	//--+-----
	//3 |3
	//4 |null
	//5 |null
	//(3 rows)
	resultSet := session.Execute(`select * from inj1 right join inj2 on a = b;`)
	// 1行, 3列;
	fmt.Println(resultSet.ToString())

	resultSet = session.Execute("explain select * from inj1 right join inj2 on a = b;")
	fmt.Println(resultSet.ToString())
}
func testHashJoin(t *testing.T, session *Session) {
	session.Execute("create table haj1 (a int primary key);")
	session.Execute("create table haj2 (b int primary key);")
	session.Execute("create table haj3 (c int primary key);")
	session.Execute("insert into haj1 values (1), (2), (3);")
	session.Execute("insert into haj2 values (2), (3), (4);")
	session.Execute("insert into haj3 values (3), (1), (9);")
	// a |b |c
	//--+--+--
	//3 |3 |3
	//(1 rows)
	resultSet := session.Execute(`select * from haj1 join haj2 on a = b join haj3 on a = c;`)
	// 1行3列;
	fmt.Println(resultSet.ToString())

	//            SQL PLAN
	//------------------------------
	// Hash Join (a = c)
	//  ->   Hash Join (a = b)
	//     ->  Seq Scan on  haj1
	//     ->  Seq Scan on  haj2
	//  ->  Seq Scan on  haj3
	resultSet = session.Execute("explain select * from haj1 join haj2 on a = b join haj3 on a = c;")
	fmt.Println(resultSet.ToString())
}
func testGroupBy(t *testing.T, session *Session) {
	session.Execute("CREATE TABLE test (a int primary key , b varchar,c float);")
	session.Execute("INSERT INTO test VALUES (1, 'aa', 1.0);")
	session.Execute("INSERT INTO test VALUES (2, 'bb', 1.0);")
	session.Execute("INSERT INTO test VALUES (3, null, NULL);")
	session.Execute("INSERT INTO test VALUES (4, 'dd', 2.0);")
	session.Execute("INSERT INTO test VALUES (5, 'ee', 2.0);")
	session.Execute("INSERT INTO test VALUES (6, 'ff', 3.0);")
	session.Execute("INSERT INTO test VALUES (7, 'gg', 3.0);")
	session.Execute("INSERT INTO test VALUES (8, 'hh', 7.8);")
	session.Execute("INSERT INTO test VALUES (9, 'ii', 7.8);")
	//total |min_a |max_b |sum_c |avg_c
	//------+------+------+------+------
	//2     |6     |gg    |6     |3
	//2     |8     |ii    |15.6  |7.8
	//2     |1     |bb    |2     |1
	//1     |3     |null  |null  |null
	//2     |4     |ee    |4     |2
	resultSet := session.Execute("select count(a) as total, min(a), max(b),sum(c),avg(c) from test group by c ;")
	toString := resultSet.ToString()
	fmt.Println(toString)

	resultSet = session.Execute("explain select count(a) as total, min(a), max(b),sum(c),avg(c) from test group by c ;")
	fmt.Println(resultSet.ToString())
}
func testGroupByOrderBy(t *testing.T, session *Session) {
	session.Execute("create table gbo1 (a int primary key, b text, c float);")
	session.Execute("insert into gbo1 values (1, 'aa', 3.1);")
	session.Execute("insert into gbo1 values (2, 'bb', 5.3);")
	session.Execute("insert into gbo1 values (3, null, NULL);")
	session.Execute("insert into gbo1 values (4, null, 4.6);")
	session.Execute("insert into gbo1 values (5, 'bb', 5.8);")
	session.Execute("insert into gbo1 values (6, 'dd', 1.4);")

	//b    |min_c |max_a |avg_c
	//-----+------+------+------
	//dd   |1.4   |6     |1.4
	//aa   |3.1   |1     |3.1
	//null |4.6   |4     |4.6
	//bb   |5.3   |5     |5.55
	resultSet := session.Execute("select b, min(c), max(a), avg(c) from gbo1 group by b order by avg_c;")
	toString := resultSet.ToString()
	// dd   1.4  6  1.4
	// aa   3.1  1  3.1
	// null 4.6  4  4.6
	// bb   5.3  5  5.55
	fmt.Println(toString)

	resultSet = session.Execute("explain select b, min(c), max(a), avg(c) from gbo1 group by b order by avg_c;")
	fmt.Println(resultSet.ToString())
}

func testExplain(t *testing.T, session *Session) {
	resultSet := session.Execute("explain insert into t3 values (70, 87, 82, 9.52);")
	fmt.Println(resultSet.ToString())
}
func testShowTableNames(t *testing.T, session *Session) {
	set := session.Execute("begin;")
	fmt.Println(set.ToString())

	set = session.Execute("create table test4(id int primary key, name varchar);")
	fmt.Println(set.ToString())

	set = session.Execute("create table test5(id int primary key, name varchar);")
	fmt.Println(set.ToString())

	set = session.Execute("create table test6(id int primary key, name varchar);")
	fmt.Println(set.ToString())

	set = session.Execute("commit;")
	fmt.Println(set.ToString())

	// showTableNames: t1,t2,t3,t4,test4,test5,test6;
	showTableNames := session.Execute("show tables;")
	fmt.Printf("showTableNames: %s;\n", showTableNames.ToString())
}

func showTableInfo(t *testing.T, session *Session, tableName string) {
	sql := fmt.Sprintf("show table %s;", tableName)
	showTableNames := session.Execute(sql)
	// COLUMNS: {
	//	a Integer PRIMARY KEY
	//	b Integer DEFAULT 100
	//	c Float DEFAULT 1.1
	//	d UNKNOWN DEFAULT false
	//	e UNKNOWN DEFAULT true
	//	f String DEFAULT v1
	//	g String DEFAULT v2
	//	h String DEFAULT v3
	//}
	fmt.Printf("show Table %s : %s \n", tableName, showTableNames.ToString())
}
func testShowAllTableRows(t *testing.T, session *Session) {
	showTableNames := session.ShowTableNames()
	tables := strings.Split(showTableNames, ",")
	for _, tableName := range tables {
		if tableName == "" {
			continue
		}
		fmt.Printf("\nScan table %s:\n", tableName)
		sql := fmt.Sprintf("select * from %s;", tableName)
		resultSet := session.Execute(sql)
		fmt.Println(resultSet.ToString())
	}
}

func TestMemoryStorage(t *testing.T) {
	memoryStorage := storage.NewMemoryStorage()
	server := NewServer(memoryStorage)
	session := server.Session()
	// 第一组测试
	testCreateTable(t, session)
	testInsertTable(t, session) // t1 t2 t3 t4;
	testUpdate(t, session)      // t2
	showTableInfo(t, session, "t2")
	testDelete(t, session)
	testOrderBy(t, session)

	//第二组测试
	testAgg(t, session)
	testFilter(t, session)
	testIndexScan(t, session)
	testPrimaryKeyScan(t, session)

	//第三组测试
	testCrossJoin(t, session)
	testInnerJoin(t, session)
	testHashJoin(t, session)

	// 第四组测试
	testGroupBy(t, session)
	testGroupByOrderBy(t, session)

	// 第五组测试
	// testExplain(t, session)

	// 第六组测试
	testShowAllTableRows(t, session)
	testShowTableNames(t, session)

	// TRANSACTION 136 COMMIT;
}

func TestDiskStorage(t *testing.T) {
	util.ClearPath(dirPath)
	diskStorage := storage.NewDiskStorage(dirPath)
	server := NewServer(diskStorage)
	session := server.Session()

	// 第一组测试
	testCreateTable(t, session)
	testInsertTable(t, session) // t1 t2 t3 t4;
	testUpdate(t, session)
	testDelete(t, session)
	testOrderBy(t, session)

	// 第二组测试
	testAgg(t, session)
	testFilter(t, session)
	testIndexScan(t, session)
	testPrimaryKeyScan(t, session)

	// 第三组测试
	testCrossJoin(t, session)
	testInnerJoin(t, session)
	testHashJoin(t, session)

	// 第四组测试
	testGroupBy(t, session)
	testGroupByOrderBy(t, session)

	// 第五组测试
	testExplain(t, session)

	// 第六组测试
	testShowAllTableRows(t, session)
	testShowTableNames(t, session)

	// TRANSACTION 136 COMMIT;
}
