package sql

import (
	"github.com/kebukeYi/TrainSQL/storage"
)

type ServerManager struct {
	// 统一事务管理器;
	txnManager *storage.TransactionManager
}

func (s *ServerManager) Begin() Service {
	return NewKVService(s.txnManager.Begin())
}

func (s *ServerManager) Close() error {
	return s.txnManager.Close()
}

func NewServer(sto storage.Storage) *ServerManager {
	return &ServerManager{
		txnManager: storage.NewTransactionManager(sto),
	}
}
func (s *ServerManager) Session() *Session {
	return &Session{
		Server: s,
	}
}
