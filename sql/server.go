package sql

import (
	"github.com/kebukeYi/TrainSQL/storage"
)

type Server struct {
	// 统一事务管理器
	txnManager *storage.TransactionManager
}

func (s *Server) Begin() Service {
	return NewKVService(s.txnManager.Begin())
}

func NewServer(sto storage.Storage) *Server {
	return &Server{
		txnManager: storage.NewTransactionManager(sto),
	}
}
func (s *Server) Session() Session {
	return Session{
		Server: s,
	}
}
