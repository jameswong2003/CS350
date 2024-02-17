package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// errcode examples of TaskResponse
const (
	ErrSuccess = iota
	ErrWait
	ErrAllDone
)

// Type of Task.TaskType
const (
	TypeMap = iota + 1
	TypeReduce
)

// Status of Task
const (
	StatusReady = iota + 1
	StatusSent
	StatusFinish
)

type Task struct {
	TaskId   int32
	TaskType int32
	Content  string // map => file_name, reduce => reduce number
	Status   int32
}

type TaskRequest struct {
}

type TaskResponse struct {
	ErrCode int32
	Task    Task
}

type NotifyRequest struct {
	TaskId   int32
	TaskType int32
}

type NotifyResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
