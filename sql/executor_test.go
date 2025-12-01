package sql

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/storage"
	"testing"
)

func TestGroupBy(t *testing.T) {
	memoryStorage := storage.NewMemoryStorage()
	serverManager := NewServer(memoryStorage)
	session := serverManager.Session()
	session.Execute("CREATE TABLE test (a int primary key , b varchar,c float);")
	session.Execute("INSERT INTO test VALUES (1, 'aa', 1.0);")
	session.Execute("INSERT INTO test VALUES (2, 'bb', 1.0);")
	session.Execute("INSERT INTO test VALUES (3, null, NULL);")
	session.Execute("INSERT INTO test VALUES (4, 'dd', 2.0);")
	session.Execute("INSERT INTO test VALUES (5, 'ee', 2.0);")
	session.Execute("INSERT INTO test VALUES (6, 'ff', 3.0);")
	session.Execute("INSERT INTO test VALUES (7, 'gg', 3.0);")
	session.Execute("INSERT INTO test VALUES (8, 'hh', 7.8);")
	session.Execute("INSERT INTO test VALUES (9, 'ii', 7.8);")
	//resultSet := session.Execute("select count(a) as total, min(a), max(b),sum(c),avg(c) from test group by a ;")
	resultSet := session.Execute("select count(a) as total, min(a), max(b),sum(c),avg(c) from test group by c ;")
	toString := resultSet.ToString()
	fmt.Println(toString)
}
