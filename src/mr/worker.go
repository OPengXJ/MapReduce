package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"plugin"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		CallHandOutTask(mapf, reducef)
		time.Sleep(1 * time.Second)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallHandOutTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := HandOutTaskReply{}
	args := HandOutTaskArgs{}
	call("Master.HandOutTask", &args, &reply)
	// the task is map
	if reply.Y.TaskType == 1 {
		fileNames := HandleMap(reply.Y, mapf)
		taskDoneArgs := TaskDoneArgs{
			TaskName:  reply.Y.Taskname,
			TaskType:  1,
			FileNames: fileNames,
		}
		taskDoneReply := TaskDoneReply{}
		call("Master.TaskDone", &taskDoneArgs, &taskDoneReply)
	} else {
		fmt.Println(reply.Y.IntermediateFileNames)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

func HandleMap(task Task, mapf func(string, string) []KeyValue) []string {

	intermediate := []KeyValue{}
	filename := task.InputFileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// output intermediate to R local file
	R := task.NReduce
	fileNames := make([]string, R)
	files := make([]*os.File, R)
	for i := 0; i < R; i++ {
		filename :="mr" + "-" + strconv.Itoa(task.Taskname) + "-" + strconv.Itoa(i)
		file, _ := os.Create(filename)
		files[i] = file
		fileNames[i] = filename
	}

	for _, kv := range intermediate {
		// make a key or some same key to same file
		fmt.Println(kv.Key,R)
		index := ihash(kv.Key) % R
		enc := json.NewEncoder(files[index])
		enc.Encode(&kv)
	}
	return fileNames
}
