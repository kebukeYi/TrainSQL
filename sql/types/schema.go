package types

type Table struct {
	Name    string
	Columns []ColumnV
}

type ColumnV struct {
	Name         string
	DataType     DataType
	Nullable     bool
	DefaultValue Value
}
