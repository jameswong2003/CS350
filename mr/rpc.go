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

// Error Codes for RPC Calls
const (
	ErrorSuccess = iota
	ErrorWait
	ErrorAllDone
)

// Task Status
const (
	StatusReady = iota
	StatusSent
	StatusDone
)

// Task Types
const (
	TaskMap = iota
	TaskReduce
)

type Task struct {
	TaskId   int32
	TaskType int32
	Content  string
	Status   int32
}

type TaskRequest struct {
}

type TaskResponse struct {
	ErrorCode int32
	Task      Task
}

type ReportRequest struct {
	TaskId   int32
	TaskType int32
}

type ReportResponse struct {
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
