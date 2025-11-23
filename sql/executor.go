package sql

import (
	"github.com/kebukeYi/TrainSQL/sql/types"
)

type Executor interface {
	Execute(service Service) types.ResultSet
}
