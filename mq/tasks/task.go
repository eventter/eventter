package tasks

type Task struct {
	ID     uint64
	Name   string
	Data   interface{}
	Cancel func()
	Err    error
}
