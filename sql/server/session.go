package server

import (
	"github.com/kebukeYi/TrainSQL/sql/parser"
	"github.com/kebukeYi/TrainSQL/sql/plan"
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
)

type Session struct {
	Server Server
}

func (s *Session) Execute(sqlStr string) types.ResultSet {
	newParser := parser.NewParser(sqlStr)
	statement := newParser.Parse()
	if statement != nil {
		service := s.Server.Begin()
		plan := plan.Plan{
			Service: service,
		}
		node := plan.BuildNode(statement)
		e := plan.BuildExecutor(node)
		resultSet := e.Execute(service)
		if resultSet != nil {
			service.Commit()
		} else {
			service.Rollback()
		}
		return resultSet
	} else {
		util.Error("sql parse statement is nil!")
	}
	return nil
}
