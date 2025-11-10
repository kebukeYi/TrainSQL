package simple_db

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/metadata_manager"
	"github.com/kebukeYi/TrainSQL/parser"
	"github.com/kebukeYi/TrainSQL/planner"
	"github.com/kebukeYi/TrainSQL/query"
	"github.com/kebukeYi/TrainSQL/tx"
	"github.com/kebukeYi/TrainSQL/util"
	"testing"
)

func PrintStudentTable(tx *tx.Translation, mdm *metadata_manager.MetaDataManager) {
	queryStr := "select id, name, majorid, gradyear,grad from STUDENT;"
	p := parser.NewSQLParser(queryStr) //
	queryData := p.Select()            //
	test_planner := planner.CreateBasicQueryPlanner(mdm)
	test_plan := test_planner.CreatePlan(queryData, tx)
	test_interface := (test_plan.StartScan())
	test_scan, _ := test_interface.(query.Scan)
	for test_scan.Next() {
		fmt.Printf("name: %s, majorid: %d, gradyear: %d\n",
			test_scan.GetString("name"), test_scan.GetInt("majorid"),
			test_scan.GetInt("gradyear"))
	}
}

func TestCreateInsertUpdateByUpdatePlanner(t *testing.T) {
	util.ClearDir(util.PlannerDirectory)
	option := &DBOptions{
		DBDirectory:     util.PlannerDirectory,
		BlockSize:       2048,
		BufferSize:      30,
		LogFilePathName: "logfile.log",
	}
	db := NewDBWithOptions(option)
	defer db.Close()
	tx := db.NewTranslation()
	mdm := metadata_manager.NewMetaDataManager(db.file_manager.IsNew(), tx)
	// 提供修改功能的planner;
	updatePlanner := planner.NewBasicUpdatePlanner(mdm)
	createTableSql := "create table STUDENT (id int, name varchar(16), majorid int, gradyear int,grad varchar(10));"
	p := parser.NewSQLParser(createTableSql)
	tableData := p.ParseCommand().(*parser.CreateTableData)
	// 调用TableManager创建表;
	updatePlanner.ExecuteCreateTable(tableData, tx)

	insertSQL := "insert into STUDENT (id, name, majorid, gradyear, grad) values(10,\"tylor\", 10, 2020,\"A\");"
	p = parser.NewSQLParser(insertSQL)
	insertData := p.ParseCommand().(*parser.InsertData)
	updatePlanner.ExecuteInsert(insertData, tx)

	insertSQL = "insert into STUDENT (id, name, majorid, gradyear, grad) values(10,\"tom\", 10, 2021,\"B\");"
	p = parser.NewSQLParser(insertSQL)
	insertData = p.ParseCommand().(*parser.InsertData)
	updatePlanner.ExecuteInsert(insertData, tx)

	insertSQL = "insert into STUDENT (id, name, majorid, gradyear, grad) values(20,\"mike\", 10, 2022,\"A\");"
	p = parser.NewSQLParser(insertSQL)
	insertData = p.ParseCommand().(*parser.InsertData)
	updatePlanner.ExecuteInsert(insertData, tx)

	insertSQL = "insert into STUDENT (id, name, majorid, gradyear, grad) values(20,\"jone\", 50, 2023,\"C\");"
	p = parser.NewSQLParser(insertSQL)
	insertData = p.ParseCommand().(*parser.InsertData)
	updatePlanner.ExecuteInsert(insertData, tx)

	insertSQL = "insert into STUDENT (id, name, majorid, gradyear, grad) values(20,\"que\", 35, 2024,\"C\");"
	p = parser.NewSQLParser(insertSQL)
	insertData = p.ParseCommand().(*parser.InsertData)
	updatePlanner.ExecuteInsert(insertData, tx)

	fmt.Println("table after insert:")
	PrintStudentTable(tx, mdm)

	updateSQL := "update STUDENT set name=\"newtylor\" where majorid=10 and gradyear=2020;"
	p = parser.NewSQLParser(updateSQL)
	updateData := p.ParseCommand().(*parser.ModifyData)
	updatePlanner.ExecuteModify(updateData, tx)

	fmt.Println("table after update:")
	PrintStudentTable(tx, mdm)

	deleteSQL := "delete from STUDENT where majorid=35;"
	p = parser.NewSQLParser(deleteSQL)
	deleteData := p.ParseCommand().(*parser.DeleteData)
	updatePlanner.ExecuteDelete(deleteData, tx)

	fmt.Println("table after delete")
	PrintStudentTable(tx, mdm)
	tx.Commit()
}

func TestSelectStudentPlan(t *testing.T) {
	option := &DBOptions{
		DBDirectory:     util.PlannerDirectory,
		BlockSize:       2048,
		BufferSize:      30,
		LogFilePathName: "logfile.log",
	}
	db := NewDBWithOptions(option)
	tx := db.NewTranslation()
	defer db.Close()
	mdm := metadata_manager.NewMetaDataManager(db.file_manager.IsNew(), tx)
	queryStr := "select id, name, majorid, gradyear,grad from STUDENT where id = majorid and grad=\"A\";"
	//queryStr := "select id, name, majorid, gradyear,grad from STUDENT;"
	p := parser.NewSQLParser(queryStr) //
	queryData := p.Select()            // 解析sql语句; 并把where条件 组装起来;
	// prd [term,term,term,term{left,right}]
	test_planner := planner.CreateBasicQueryPlanner(mdm)
	test_plan := test_planner.CreatePlan(queryData, tx)
	test_interface := (test_plan.StartScan())
	test_scan, _ := test_interface.(query.Scan)
	for test_scan.Next() {
		fmt.Printf("name: %s, majorid: %d, gradyear: %d\n",
			test_scan.GetString("name"), test_scan.GetInt("majorid"),
			test_scan.GetInt("gradyear"))
	}
	tx.Commit()
}

func TestIndex(t *testing.T) {
	util.ClearDir(util.IndexDirectory)
	option := &DBOptions{
		DBDirectory:     util.IndexDirectory,
		BlockSize:       2048,
		BufferSize:      30,
		LogFilePathName: "logfile.log",
	}
	db := NewDBWithOptions(option)
	tx := db.NewTranslation()

	mdm := metadata_manager.NewMetaDataManager(db.file_manager.IsNew(), tx)

	//创建 student 表，并插入一些记录;
	updatePlanner := planner.NewBasicUpdatePlanner(mdm)
	createTableSql := "create table STUDENT (name varchar(16), majorid int, gradyear int);"
	p := parser.NewSQLParser(createTableSql)
	tableData := p.ParseCommand().(*parser.CreateTableData)
	updatePlanner.ExecuteCreateTable(tableData, tx)

	insertSQL := "insert into STUDENT (name, majorid, gradyear) values(\"tylor\", 30, 2020);"
	p = parser.NewSQLParser(insertSQL)
	insertData := p.ParseCommand().(*parser.InsertData)
	updatePlanner.ExecuteInsert(insertData, tx)

	insertSQL = "insert into STUDENT (name, majorid, gradyear) values(\"tom\", 35, 2023);"
	p = parser.NewSQLParser(insertSQL)
	insertData = p.ParseCommand().(*parser.InsertData)
	updatePlanner.ExecuteInsert(insertData, tx)

	fmt.Println("table after insert:")
	PrintStudentTable(tx, mdm)

	// 在 student 表的 majorid 字段建立索引;
	fmt.Println("start create index table:")
	mdm.CreateIndex("majoridIdx", "STUDENT", "majorid", tx)
	// 查询建立在 student 表上的索引并根据索引输出对应的记录信息;
	studetPlan := planner.NewTablePlan(tx, "STUDENT", mdm)
	updateScan := studetPlan.StartScan().(*query.TableScan)

	// 先获得当前表的所有的索引列对象; 这里我们只有 majorid 列建立了索引对象;
	indexes := mdm.GetIndexInfo("STUDENT", tx)

	//获取 majorid 对应的索引对象
	majoridIdxInfo := indexes["majorid"]
	// 将改 rid 加入到索引表;
	majorIdx := majoridIdxInfo.Open()
	updateScan.BeforeFirst() // 源数据表重新置位;
	for updateScan.Next() {  //源数据表的所有记录;
		// 返回当前记录的 rid
		dataRID := updateScan.GetRid()          // 获得每条记录的磁盘地址;
		dataVal := updateScan.GetVal("majorid") // 获得索引列的值;
		// majoridIdx#12.tbl : insert into ... ;
		majorIdx.Insert(dataVal, dataRID) // 索引表的插入;
	}

	// 通过索引表, 获得给定字段内容的记录;
	majorid := 35
	majorIdx.BeforeFirst(query.NewConstantWithInt(&majorid))
	for majorIdx.Next() {
		datarid := majorIdx.GetDataRID()
		updateScan.MoveToRid(datarid)
		fmt.Printf("student name :%s, id: %d\n",
			updateScan.GetScan().GetString("name"),
			updateScan.GetScan().GetInt("majorid"))
	}

	majorIdx.Close()
	updateScan.GetScan().Close()
	tx.Commit()
}
