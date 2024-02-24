package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const idleTimeAllowed = time.Second * 10

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
	nReduce   int32
	status    int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(req *TaskRequest, resp *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// All the tasks are completed
	if c.status == Completed {
		resp.ErrorCode = ErrorAllDone
		return nil
	}

	taskNotCompleted := false
	n := len(c.TaskQueue)
	for i := 0; i < n; i++ {
		currentTask := c.TaskQueue[c.CurrentId]
		c.CurrentId = (c.CurrentId + 1) % n

		if currentTask.Status == StatusReady {
			currentTask.Status = StatusSent

			resp.Task = *currentTask
			resp.ErrorCode = ErrorSuccess
			go checkTask(currentTask.TaskId, currentTask.TaskType, c)
			return nil
		} else if currentTask.Status == StatusSent {
			taskNotCompleted = true
		}
	}

	// If some tasks are still not completed
	if taskNotCompleted {
		resp.ErrorCode = ErrorWait
		return nil
	}

	// If all tasks are Done
	switch c.status {
	case Mapping:
		c.status = Reducing
		importReduceTasks(c)
		resp.ErrorCode = ErrorWait
		return nil
	case Reducing:
		c.status = Completed
		resp.ErrorCode = ErrorAllDone
		return nil
	}
	return nil
}

func checkTask(taskId int32, taskType int32, c *Coordinator) {
	time.Sleep(idleTimeAllowed)
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.TaskQueue[taskId].TaskType != taskType {
		return
	}
	if c.TaskQueue[taskId].Status == StatusSent {
		c.TaskQueue[taskId].Status = StatusReady
	}
}

func (c *Coordinator) Report(args *ReportRequest, reply *ReportResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.TaskQueue[args.TaskId].Status = StatusDone
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status == Completed {
		ret = true
	}

	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue: make([]*Task, 0),
		CurrentId: 0,
		status:    Mapping,
		nReduce:   int32(nReduce),
	}

	// Your code here.
	// Load Map Tasks
	importMapTasks(&c, files)
	c.server()
	return &c
}

func importMapTasks(c *Coordinator, files []string) {
	c.TaskQueue = make([]*Task, 0)
	c.CurrentId = 0
	n := len(files)

	for i := 0; i < n; i++ {
		c.TaskQueue = append(c.TaskQueue, &Task{
			TaskId:   int32(i),
			TaskType: TaskMap,
			Content:  files[i],
			Status:   StatusReady,
		})
	}
}

func importReduceTasks(c *Coordinator) {
	c.TaskQueue = make([]*Task, 0)
	c.CurrentId = 0

	for i := 0; i < int(c.nReduce); i++ {
		c.TaskQueue = append(c.TaskQueue, &Task{
			TaskId:   int32(i),
			TaskType: TaskReduce,
			Content:  fmt.Sprint(c.nReduce),
			Status:   StatusReady,
		})
	}
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
