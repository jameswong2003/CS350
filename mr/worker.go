package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		req := TaskRequest{}
		res := TaskResponse{}

		if !call("Coordinator.GetTask", &req, &res) {
			fmt.Print("Call failed")
			break
		} else {
			switch res.ErrorCode {
			case ErrorWait:
				time.Sleep(1 * time.Second)
				continue
			case ErrorSuccess:
				switch res.Task.TaskType {
				case TaskMap:
					StartMapping(res.Task, mapf)
				case TaskReduce:
					StartReducing(res.Task, reducef)
				}
			case ErrorAllDone:
				break
			}
		}
	}
}

// Store intermediate key value pairs {"key", "1"} in temp files labled: "mrmapped-*""
func StartMapping(t Task, mapf func(string, string) []KeyValue) {
	fileName := t.Content
	file, err := os.Open(fileName)

	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	fileContent, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	file.Close()

	kva := mapf(fileName, string(fileContent))
	oname := "mr-out-" + strconv.Itoa(int(t.TaskId))
	tmpfile, err := ioutil.TempFile(".", "temp-"+oname)
	if err != nil {
		log.Fatalf("no temp file exist %v", err)
	}

	enc := json.NewEncoder(tmpfile)
	for _, kv := range kva {
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("[DoMap]encode save json err=%v\n", err)
		}
	}
	tmpfile.Close()
	os.Rename(tmpfile.Name(), oname)
	ReportToCoordinator(t.TaskId, t.TaskType)
}

func StartReducing(t Task, reducef func(string, []string) string) {
	var kva []KeyValue
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatalf("No files exist in current Director %v", err)
	}

	for _, file := range files {
		matched, _ := regexp.Match(`^mr-out-*`, []byte(file.Name()))
		if !matched {
			continue
		}

		fileName := file.Name()
		file, err := os.Open(fileName)

		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}

		nReduce, _ := strconv.Atoi(t.Content)
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if ihash(kv.Key)%nReduce == int(t.TaskId) {
				kva = append(kva, kv)
			}
		}
	}

	sort.Sort(ByKey(kva))
	oname := "mr-out-" + strconv.Itoa(int(t.TaskId))
	tmpfile, err := ioutil.TempFile(".", "temp-"+oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	tmpfile.Close()
	os.Rename(tmpfile.Name(), oname)

	ReportToCoordinator(t.TaskId, t.TaskType)
}

// Send a report call to coordinator, telling them that the task is completed
func ReportToCoordinator(taskId int32, taskType int32) {
	req := ReportRequest{
		TaskId:   taskId,
		TaskType: taskType,
	}

	resp := ReportResponse{}

	call("Coordinator.Report", &req, &resp)
}

// uncomment to send the Example RPC to the coordinator.
// CallExample()
// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}
