package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	intermediate := []KeyValue{}
	fileName := t.Content
	file, err := os.Open(fileName)
	defer file.Close()

	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	fileContent, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	kva := mapf(fileName, string(fileContent))
	oname := "mr-out-" + strconv.Itoa(int(t.TaskId))
	tmpfile, err := ioutil.TempFile(".", "temp_"+oname)

	enc := json.NewEncoder(tmpfile)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Error encoding KV to temp file", err)
		}
	}

	tmpfile.Close()
	os.Rename(tmpfile.Name(), oname)
}

func StartReducing(t Task, reducef func(string, []string) string) {
	var kva []KeyValue
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatalf("No files exist in current Director", err)
	}

	for _, file := range files {

	}
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
