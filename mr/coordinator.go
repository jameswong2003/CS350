package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	Mapping = iota
	Reducing
	Completed
)

type Coordinator struct {
	// Your definitions here.
	mu        sync.Mutex
	TaskQueue []*Task // Array of all tasks given to coordinator
	CurrentId int     // Current Free task
	status    int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(req *TaskRequest, resp *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status == Completed {
		return nil
	}

	n := len(c.TaskQueue)
	for i := 0; i < n; i++ {
		currentTask := c.TaskQueue[c.CurrentId]
		c.CurrentId = (c.CurrentId + 1) % n

		if currentTask.Status == StatusReady {
			currentTask.Status = StatusSent

			resp.Task = *currentTask
			resp.ErrorCode = ErrorSuccess

			return nil
		} else if currentTask.Status == StatusSent {
			resp.ErrorCode = ErrorWait
			return nil
		}
	}

	// If all tasks are Done
	switch c.status {
	case Mapping:
		c.status = Reducing
		return nil
	case Reducing:
		c.status = Completed
		return nil
	}
	return nil
}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		status: Mapping,
	}

	// Your code here.

	c.server()
	return &c
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
