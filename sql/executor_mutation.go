package sql

import (
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
)

type InsertTableExecutor struct {
	TableName string
	Columns   []string
	Values    [][]*types.Expression
}

func NewInsertTableExecutor(tableName string, columns []string, values [][]*types.Expression) *InsertTableExecutor {
	return &InsertTableExecutor{
		TableName: tableName,
		Columns:   columns,
		Values:    values,
	}
}

// 列对齐自动填充;
// tbl:
// insert into tbl values(1, 2, 3);
// a       b       c      d
// 1       2       3     无指定值,则 default 填充;
func padRow(table *types.Table, row types.Row) (types.Row, error) {
	for id, column := range table.Columns {
		if id >= len(row) {
			if column.DefaultValue == nil {
				return nil, util.Error("[padRow] Column %s has no default value;\n", column.Name)
			} else {
				row = append(row, column.DefaultValue)
			}
		}
	}
	return row, nil
}

// tbl:
// insert into tbl(d, c) values(1, 2);
//
//	a          b       c          d
//
// default   default   2          1
func makeRow(table *types.Table, columns []string, row types.Row) (types.Row, error) {
	// 判断列数是否和value数一致
	if len(columns) != len(row) {
		return nil, util.Error("[makeRow] Columns and values count not match;\n")
	}
	input := make(map[string]types.Value)
	for i, column := range columns {
		input[column] = row[i]
	}
	var newRow []types.Value
	for _, column := range table.Columns {
		if input[column.Name] == nil {
			if column.DefaultValue == nil {
				return nil, util.Error("[makeRow] Column %s has no default value;\n", column.Name)
			} else {
				input[column.Name] = column.DefaultValue
				newRow = append(newRow, input[column.Name])
			}
		} else {
			newRow = append(newRow, input[column.Name])
		}
	}
	return newRow, nil
}

func (i *InsertTableExecutor) Execute(s Service) types.ResultSet {
	count := 0
	// 先取出表信息;
	mustGetTable, err := s.MustGetTable(i.TableName)
	if err != nil {
		return &types.ErrorResult{
			ErrorMessage: err.Error(),
		}
	}
	// 每一行数据;
	for _, expressions := range i.Values {
		var row []types.Value
		// 每一行的多个列;
		for _, expression := range expressions {
			row = append(row, expression.ConstVal)
		}
		// 如果没有指定插入的列;
		if i.Columns == nil {
			row, err = padRow(mustGetTable, row)
			if err != nil {
				return &types.ErrorResult{
					ErrorMessage: err.Error(),
				}
			}
		} else {
			// 指定了插入的列，需要对 value 信息进行整理
			row, err = makeRow(mustGetTable, i.Columns, row)
			if err != nil {
				return &types.ErrorResult{
					ErrorMessage: err.Error(),
				}
			}
		}
		err = s.CreateRow(i.TableName, row)
		if err != nil {
			return &types.ErrorResult{ErrorMessage: err.Error()}
		}
		count++
	}
	return &types.InsertTableResult{
		Count: count,
	}
}

type UpdateTableExecutor struct {
	TableName string
	Source    Executor
	columns   map[string]*types.Expression
}

func NewUpdateTableExecutor(tableName string, source Executor, columns map[string]*types.Expression) *UpdateTableExecutor {
	return &UpdateTableExecutor{
		TableName: tableName,
		Source:    source,
		columns:   columns,
	}
}
func (u *UpdateTableExecutor) Execute(s Service) types.ResultSet {
	update := 0
	var result types.ResultSet
	result = u.Source.Execute(s)
	switch result.(type) {
	case *types.ScanTableResult:
		table, err := s.MustGetTable(u.TableName)
		if err != nil {
			return &types.ErrorResult{
				ErrorMessage: err.Error(),
			}
		}
		selectTableResult := result.(*types.ScanTableResult)
		// 遍历所有需要更新的行;
		for _, row := range selectTableResult.Rows {
			// update user set name='kk' where id = 1; // 可能存在多行需要更新;
			pKValue := table.GetPrimaryKeyOfValue(row)
			// 不清楚要具体更新哪些列,因此需要全部判断;
			for i, column := range selectTableResult.Columns {
				if expr, ok := u.columns[column]; ok {
					// 只更新特定列的值;
					row[i] = expr.ConstVal
				}
			}
			// 执行更新操作;
			// 1.如果有主键更新: 删除原来的数据, 新增一条新的数据;
			// 2.否则就 table_name + primary key => 更新数据;
			// 所有行的存储结构是: tableName_primaryKey_
			err = s.UpdateRow(table, pKValue, row)
			if err != nil {
				return &types.ErrorResult{ErrorMessage: err.Error()}
			}
			update++
		}
	default:
		return &types.ErrorResult{
			ErrorMessage: util.Error("#UpdateTableExecutor Unsupported result type: %T\n", result).Error(),
		}
	}
	return &types.UpdateTableResult{
		Count: update,
	}
}

type DeleteTableExecutor struct {
	TableName string
	Source    Executor
}

func NewDeleteTableExecutor(tableName string, source Executor) *DeleteTableExecutor {
	return &DeleteTableExecutor{
		TableName: tableName,
		Source:    source,
	}
}
func (d *DeleteTableExecutor) Execute(s Service) types.ResultSet {
	count := 0
	var result types.ResultSet
	result = d.Source.Execute(s)
	// 执行扫描操作，获取到扫描的结果;
	switch result.(type) {
	case *types.ScanTableResult:
		table, err := s.MustGetTable(d.TableName)
		if err != nil {
			return &types.ErrorResult{
				ErrorMessage: err.Error(),
			}
		}
		selectTableResult := result.(*types.ScanTableResult)
		// 遍历所有需要更新的行;
		for _, row := range selectTableResult.Rows {
			// update user set name='kk' where id = 1; // 可能存在多行需要更新;
			pKValue := table.GetPrimaryKeyOfValue(row)
			err = s.DeleteRow(table, pKValue)
			if err != nil {
				return &types.ErrorResult{ErrorMessage: err.Error()}
			}
			count++
		}
	default:
		return &types.ErrorResult{
			ErrorMessage: util.Error("[UpdateTableExecutor] Unsupported result type: %T\n", result).Error(),
		}
	}
	return &types.DeleteTableResult{
		Count: count,
	}
}
