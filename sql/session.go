package sql

import (
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
)

type Session struct {
	Server  Server
	Service Service
}

func (s *Session) Execute(sqlStr string) types.ResultSet {
	newParser := NewParser(sqlStr)
	statement := newParser.Parse()
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
			return &types.CommitResult{Version: int(version)}
		case *RollbackData:
			version := s.Service.Version()
			s.Service.Rollback()
			return &types.RollbackResult{Version: int(version)}
		case *ExplainData:
		default:
			if s.Service != nil {
				plan := NewPlan(statement, s.Service)
				return plan.Execute()
			} else {
				s.Service = s.Server.Begin()
				plan := NewPlan(statement, s.Service)
				resultSet := plan.Execute()
				if resultSet != nil {
					plan.Service.Commit()
					return resultSet
				} else {
					plan.Service.Rollback()
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
	return nil
}

func (s *Session) GetTable(tableName string) string {
	if s.Service != nil {
		table := s.Service.MustGetTable(tableName)
		s.Service.Commit()
		return table.ToString()
	} else {
		service := s.Server.Begin()
		table := service.MustGetTable(tableName)
		service.Commit()
		return table.ToString()
	}
}

func (s *Session) GetTableName() string {
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
