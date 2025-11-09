package metadata_manager

import (
	"fmt"
	bmg "github.com/kebukeYi/TrainSQL/buffer_manager"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lm "github.com/kebukeYi/TrainSQL/log_manager"
	record_mgr "github.com/kebukeYi/TrainSQL/record_manager"
	"github.com/kebukeYi/TrainSQL/tx"
	"github.com/kebukeYi/TrainSQL/util"
	"testing"
)

func TestTableManager(t *testing.T) {
	util.ClearDir(util.MeatDataManageTestDirectory)
	file_manager, _ := fm.NewFileManager(util.MeatDataManageTestDirectory, 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
	buffer_manager := bmg.NewBufferManager(file_manager, log_manager, 3)

	tx := tx.NewTransation(file_manager, log_manager, buffer_manager)

	sch := record_mgr.NewSchema()
	sch.AddIntField("A")       // 默认8 字节大小
	sch.AddStringField("B", 9) // string 9 字节大小

	// 1.创建表元数据(内存版)
	// 2.创建表元数据(磁盘版)
	tm := NewTableManager(true, tx)
	// 创建用户表;
	tm.CreateTable("MyTable", sch, tx)
	// 获得用户表的元数据(字段名字,字段类型,字段大小,字段位移)
	layout := tm.GetLayout("MyTable", tx)
	size := layout.SlotSize() // 每条记录的固定长度;
	sch2 := layout.Schema()
	fmt.Printf("MyTable has slot size: %d\n", size)
	fmt.Println("Its fields are: ")
	for _, fldName := range sch2.Fields() {
		fldType := ""
		if sch2.Type(fldName) == record_mgr.INTEGER {
			fldType = "int"
		} else {
			strlen := sch2.Length(fldName)
			fldType = fmt.Sprintf("varchar( %d )", strlen)
		}
		fmt.Printf("%s : %s\n", fldName, fldType)
	}
	tx.Commit()
}
