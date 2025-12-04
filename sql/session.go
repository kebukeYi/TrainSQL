package sql

import (
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"strings"
)

type Session struct {
	Server  *ServerManager
	Service Service
}

func (s *Session) Execute(sqlStr string) types.ResultSet {
	newParser := NewParser(sqlStr)
	var err error
	statement, err := newParser.Parse()
	if err != nil {
		return &types.ErrorResult{ErrorMessage: err.Error()}
	}
	if statement != nil {
		switch statement.(type) {
		case *BeginData:
			if s.Service != nil {
				return &types.ErrorResult{
					ErrorMessage: "Transaction already exists;",
				}
			} else {
				txn := s.Server.Begin()
				s.Service = txn
				version := txn.Version()
				return &types.BeginResult{
					Version: int(version),
				}
			}
		case *CommitData:
			version := s.Service.Version()
			s.Service.Commit()
			s.Service = nil
			return &types.CommitResult{Version: int(version)}
		case *RollbackData:
			version := s.Service.Version()
			s.Service.Rollback()
			s.Service = nil
			return &types.RollbackResult{Version: int(version)}
		case *ExplainData:
			sourceStatement := statement.(*ExplainData).Statements
			if s.Service != nil {
				plan := NewPlan(sourceStatement, s.Service)
				node, err := plan.BuildNode()
				if err != nil {
					return &types.ErrorResult{
						ErrorMessage: err.Error(),
					}
				}
				explain := s.Explain(node)
				return &types.ExplainResult{
					Plan: explain,
				}
			} else {
				service := s.Server.Begin()
				plan := NewPlan(sourceStatement, service)
				node, err := plan.BuildNode()
				if err != nil {
					service.Rollback()
					return &types.ErrorResult{
						ErrorMessage: err.Error(),
					}
				}
				explain := s.Explain(node)
				service.Commit()
				return &types.ExplainResult{
					Plan: explain,
				}
			}
		case *ShowTableData:
			showStatement := statement.(*ShowTableData)
			table := s.GetTable(showStatement.TableName)
			return &types.ShowTableResult{
				TableInfo: table,
			}
		case *ShowTDataBaseData:
			tables := s.ShowTableNames()
			return &types.ShowDataBaseResult{
				TablesInfo: tables,
			}
		default:
			// 前面手动 begin 起来的事务;随后的sql会进入到这分支;
			if s.Service != nil {
				plan := NewPlan(statement, s.Service)
				return plan.Execute()
			} else {
				// 没有手动启动 begin, 那么随后的每一条sql 都将会进入到这分支;
				// 自动创建事务, 自动提交;
				s.Service = s.Server.Begin()
				plan := NewPlan(statement, s.Service)
				resultSet := plan.Execute()
				if resultSet != nil {
					plan.Service.Commit()
					s.Service = nil
					return resultSet
				} else {
					s.Service = nil
					plan.Service.Rollback()
					s.Service = nil
					return &types.ErrorResult{
						ErrorMessage: "Execute sql error will rollback;",
					}
				}
			}
		}
	} else {
		return &types.ErrorResult{
			ErrorMessage: "Parser sql error;",
		}
	}
}

func (s *Session) GetTable(tableName string) string {
	if s.Service != nil {
		table, err := s.Service.MustGetTable(tableName)
		if err != nil {
			s.Service.Rollback()
			return err.Error()
		}
		s.Service.Commit()
		return table.ToString()
	} else {
		service := s.Server.Begin()
		table, err := service.MustGetTable(tableName)
		if err != nil {
			service.Rollback()
			return err.Error()
		}
		service.Commit()
		return table.ToString()
	}
}

func (s *Session) ShowTableNames() string {
	var names []string
	if s.Service != nil {
		names = s.Service.GetTableNames()
		s.Service.Commit()
	} else {
		service := s.Server.Begin()
		names = service.GetTableNames()
		service.Commit()
	}
	return util.Join(names, ",")
}

func (s *Session) Explain(node Node) string {
	var builder strings.Builder
	node.FormatNode(&builder, "", true)
	return builder.String()
}
