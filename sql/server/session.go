package server

import (
	"practiceSQL/sql/parser"
	"practiceSQL/sql/plan"
	"practiceSQL/sql/types"
	"practiceSQL/sql/util"
)

type Session struct {
	Server Server
}

func (s *Session) Execute(sqlStr string) types.ResultSet {
	newParser := parser.NewParser(sqlStr)
	statement := newParser.Parse()
	if statement != nil {
		service := s.Server.Begin()
		plan := plan.Plan{}
		plan.BuildNode(statement)
		e := plan.GetExecutor()
		resultSet := service.execute(e)
		if resultSet != nil {
			service.commit()
		} else {
			service.rollback()
		}
		return resultSet
	} else {
		util.Error("sql parse statement is nil!")
	}
	return nil
}
