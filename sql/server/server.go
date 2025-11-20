package server

import (
	"practiceSQL/storage"
)

type Server interface {
	Begin() Service
	Session() Session
}

type KVServer struct {
	txnManager *storage.TransactionManager
}

func (s *KVServer) Begin() Service {
	return NewKVService(s.txnManager.Begin())
}

func NewKVServer(sto storage.Storage) *KVServer {
	return &KVServer{
		txnManager: storage.NewTransactionManager(sto),
	}
}
func (s *KVServer) Session() Session {
	return Session{
		Server: s,
	}
}
