package executor

import (
	"github.com/kebukeYi/TrainSQL/sql/server"
	"github.com/kebukeYi/TrainSQL/sql/types"
)

type Executor interface {
	Execute(service server.Service) types.ResultSet
}
