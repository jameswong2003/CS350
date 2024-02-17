package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const WorkerDieTime = 10 * time.Second

const (
	MapPeriod = iota + 1
	ReducePeriod
	AllDone
)

type Coordinator struct {
	// Your definitions here.
	mutex     *sync.Mutex
	taskQueue []*Task
	index     int // Index of current free task
	status    int32
	NReduce   int32
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetTask(req *TaskRequest, res *TaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.status == AllDone {
		res.ErrCode = ErrAllDone
		return nil
	}

	n := len(c.taskQueue)
	for i := 0; i < n; i++ {
		t := c.taskQueue[c.index]
		c.index = (c.index + 1) % n

		if t.Status == StatusReady {
			t.Status = StatusSent
			res.Task = *t
			res.ErrCode = ErrSuccess
			go checkTask(c, t.TaskId, t.TaskType)
			return nil
		} else if t.status == StatusSent {
			hasWaiting = true
		}
	}

	if hasWaiting {

	}
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
	c := Coordinator{}

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
