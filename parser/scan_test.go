package parser

import (
	"fmt"
	bmg "github.com/kebukeYi/TrainSQL/buffer_manager"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lm "github.com/kebukeYi/TrainSQL/log_manager"
	"github.com/kebukeYi/TrainSQL/query"
	rm "github.com/kebukeYi/TrainSQL/record_manager"
	"github.com/kebukeYi/TrainSQL/tx"
	"github.com/kebukeYi/TrainSQL/util"
	"testing"
)

func TestTableScan(t *testing.T) {
	util.ClearDir(util.QueryScanTestDirectory)
	//构造 student 表
	file_manager, _ := fm.NewFileManager(util.QueryScanTestDirectory, 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
	buffer_manager := bmg.NewBufferManager(file_manager, log_manager, 3)

	tx := tx.NewTransation(file_manager, log_manager, buffer_manager)
	sch := rm.NewSchema()

	// name 字段有 16 个字符长
	sch.AddStringField("name", 16)
	//age,id 字段为 int 类型, 默认 8字节长度;
	sch.AddIntField("age")
	sch.AddIntField("id")

	// layout 正常情况下是需要查询 tblcat, fldcat表来获得字段布局;
	// 这里直接给出现成的;
	layout := rm.NewLayoutWithSchema(sch)

	// 直接构造 student 表, 没有构建元数据;
	ts := query.NewTableScan(tx, "student", layout)
	// 插入 3 条数据
	ts.BeforeFirst()
	// 第一条记录("jim", 16, 123)
	ts.Insert()
	ts.SetString("name", "jim")
	ts.SetInt("age", 16)
	ts.SetInt("id", 123)
	//第二条记录 ("tom", 18, 567)
	ts.Insert() // 准备好插槽位移;
	ts.SetString("name", "tom")
	ts.SetInt("age", 18)
	ts.SetInt("id", 567)
	//第三条数据 hanmeimei, 19, 890
	ts.Insert()
	ts.SetString("name", "HanMeiMei")
	ts.SetInt("age", 19)
	ts.SetInt("id", 890)

	// 构造查询 student 表的 sql 语句, 这条语句包含了 select, project 两种操作;
	sql := "select id, name, age from student where id = 890;"
	//sql := "select * from student where id = 890;"
	sqlParser := NewSQLParser(sql)
	queryData := sqlParser.Select()

	// 根据 queryData 分别构造 TableScan, SelectScan, ProjectScan 并执行 sql 语句
	// 创建查询树最底部的数据表节点
	tableScan := query.NewTableScan(tx, "student", layout)
	//构造上面的 SelectScan 节点
	selectScan := query.NewSelectionScan(tableScan, queryData.Pred())
	//构造顶部 ProjectScan 节点
	projectScan := query.NewProjectScan(selectScan, queryData.Fields())
	//为遍历记录做初始化
	projectScan.BeforeFirst()
	for projectScan.Next() == true {
		//查找满足条件的记录
		fmt.Println("found record!")
		// 只要指定列的值;
		for _, field := range queryData.Fields() {
			fmt.Printf("field:%s, value:%v, ", field, projectScan.GetVal(field).ToString())
		}
		fmt.Println("")
	}
	fmt.Println("complete sql execute.")
	ts.Close()
	tx.Commit()
}
