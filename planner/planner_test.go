package planner

import (
	"fmt"
	bmg "github.com/kebukeYi/TrainSQL/buffer_manager"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lm "github.com/kebukeYi/TrainSQL/log_manager"
	"github.com/kebukeYi/TrainSQL/metadata_manager"
	"github.com/kebukeYi/TrainSQL/query"
	"github.com/kebukeYi/TrainSQL/record_manager"
	"github.com/kebukeYi/TrainSQL/tx"
	"github.com/kebukeYi/TrainSQL/util"
	"testing"
)

func printStats(n int, p Plan) {
	fmt.Printf("Here are the stats for plan p %d\n", n)
	fmt.Printf("\tR(p%d):%d\n", n, p.RecordsOutput())
	fmt.Printf("\tB(p%d):%d\n", n, p.BlocksAccessed())
}

func createStudentTable() (*tx.Translation, *metadata_manager.MetaDataManager) {
	util.ClearDir(util.PlannerDirectory)
	file_manager, _ := fm.NewFileManager(util.PlannerDirectory, 2048)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
	buffer_manager := bmg.NewBufferManager(file_manager, log_manager, 3)

	tx := tx.NewTransation(file_manager, log_manager, buffer_manager)
	sch := record_manager.NewSchema()

	sch.AddStringField("sname", 16)
	sch.AddIntField("majorId")
	sch.AddIntField("gradyear")

	mdm := metadata_manager.NewMetaDataManager(false, tx)
	mdm.CreateTable("student", sch, tx)
	layout := record_manager.NewLayoutWithSchema(sch)

	for _, field_name := range layout.Schema().Fields() {
		offset := layout.Offset(field_name)
		fmt.Printf("%s has offset %d\n", field_name, offset)
	}

	ts := query.NewTableScan(tx, "student", layout)
	fmt.Println("Filling the table with 50 random records")
	ts.BeforeFirst()
	val_for_field_sname := make([]int, 0)
	for i := 0; i < 50; i++ {
		ts.Insert() //指向一个可用插槽
		ts.SetInt("majorId", i)
		ts.SetInt("gradyear", 1990+i)
		val_for_field_sname = append(val_for_field_sname, i)
		s := fmt.Sprintf("sname_%d", i)
		ts.SetString("sname", s)
		fmt.Printf("inserting into slot %s: {%d , %s}\n", ts.GetRid().ToString(), i, s)
	}

	return tx, mdm
}

func TestNewTablePlan(t1 *testing.T) {
	// 构造 student 表
	tx, mdm := createStudentTable()
	// student 里面的所有数据
	p1 := NewTablePlan(tx, "student", mdm)

	n := 10
	// majorId = 10
	t := query.NewTerm(query.NewExpressionWithString("majorId"),
		query.NewExpressionWithConstant(query.NewConstantWithInt(&n)))
	pred := query.NewPredicateWithTerms(t)
	p2 := NewSelectPlan(p1, pred)

	n1 := 2000
	// gradyear = 2000
	t2 := query.NewTerm(query.NewExpressionWithString("gradyear"),
		query.NewExpressionWithConstant(query.NewConstantWithInt(&n1)))
	pred2 := query.NewPredicateWithTerms(t2)
	p3 := NewSelectPlan(p1, pred2)

	c := make([]string, 0)
	c = append(c, "sname")
	c = append(c, "majorId")
	c = append(c, "gradyear")
	// select sname, majorId, gradyear from student where  gradyear = 2000;
	p4 := NewProjectPlan(p3, c)

	printStats(1, p1)
	printStats(2, p2)
	printStats(3, p3)
	printStats(4, p4)
}
