package query

import (
	"fmt"
	bmg "github.com/kebukeYi/TrainSQL/buffer_manager"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lm "github.com/kebukeYi/TrainSQL/log_manager"
	rm "github.com/kebukeYi/TrainSQL/record_manager"
	"github.com/kebukeYi/TrainSQL/tx"
	"github.com/kebukeYi/TrainSQL/util"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestTableScanInsertAndDelete(t *testing.T) {
	util.ClearDir(util.QueryScanTestDirectory)
	file_manager, _ := fm.NewFileManager(util.QueryScanTestDirectory, 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile.log")
	buffer_manager := bmg.NewBufferManager(file_manager, log_manager, 3)

	tx := tx.NewTransation(file_manager, log_manager, buffer_manager)
	sch := rm.NewSchema()

	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := rm.NewLayoutWithSchema(sch)
	for _, field_name := range layout.Schema().Fields() {
		offset := layout.Offset(field_name)
		fmt.Printf("%s has offset %d\n", field_name, offset)
	}

	ts := NewTableScan(tx, "T", layout)
	fmt.Println("Filling the table with 50 random records")
	ts.BeforeFirst()
	val_for_field_A := make([]int, 0)
	for i := 0; i < 50; i++ {
		ts.Insert() // 指向一个可用插槽
		n := rand.Intn(50)
		ts.SetInt("A", n)
		val_for_field_A = append(val_for_field_A, n)
		s := fmt.Sprintf("rec%d", n)
		ts.SetString("B", s)
		fmt.Printf("inserting into slot %s: {%d , %s}\n", ts.GetRid().ToString(), n, s)
	}

	ts.BeforeFirst()
	//测试插入的内容是否正确
	slot := 0
	for ts.Next() {
		a := ts.GetInt("A")
		b := ts.GetString("B")
		require.Equal(t, a, val_for_field_A[slot])
		require.Equal(t, b, fmt.Sprintf("rec%d", a))
		slot += 1
	}

	fmt.Println("Deleting records with A-values < 25")
	count := 0
	ts.BeforeFirst()
	for ts.Next() {
		a := ts.GetInt("A")
		b := ts.GetString("B")
		if a < 25 {
			count += 1
			fmt.Printf("slot %s : { %d , %s}\n", ts.GetRid().ToString(), a, b)
			ts.Delete()
		}
	}

	fmt.Println("Here are the remaining records:")
	ts.BeforeFirst()
	for ts.Next() {
		a := ts.GetInt("A")
		b := ts.GetString("B")
		require.Equal(t, a >= 25, true)
		fmt.Printf("slot %s : { %d , %s }\n", ts.GetRid().ToString(), a, b)
	}
	tx.Commit()
	ts.Close()
}
