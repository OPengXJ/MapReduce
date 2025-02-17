package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	state               int // MASTER_INIT;MAP_FINISHED;REDUCE_FINISHED
	mapTask             []*Task
	reduceTask          map[int]*Task
	nMap                int // M
	nReduce             int //R
	mapTaskFinishNum    int
	reduceTaskFinishNum int
	mu                  sync.Mutex
}

type Task struct {
	TaskType              int // 1.map 2.reduce
	State                 int //TASK_INIT;TASK_PROCESSING;TASK_DONE
	InputFileName         string
	IntermediateFileNames []string
	Taskname              int
	NReduce               int // same as Master's

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// a function for worker to ask a work
func (m *Master) HandOutTask(args *HandOutTaskArgs, reply *HandOutTaskReply) error {
	// if the map task hasn't done,do map task first
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state < 1 {
		for _, task := range m.mapTask {
			if task.State == 0 {
				task.State = 1
				reply.Y = *task
				go m.Check(1, task.Taskname)
				return nil
			}
		}
	} else {
		for _, v := range m.reduceTask {
			if v.State == 0 {
				v.State = 1
				reply.Y = *v
				go m.Check(2, v.Taskname)
				return nil
			}
		}
	}
	reply.Y = Task{}
	return fmt.Errorf("tasks have hand out all")
}

func (m *Master) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.TaskType == 1 {
		// late's task
		if m.mapTask[args.TaskName].State == 2 {
			return nil
		}
		taskName := args.TaskName
		m.mapTask[taskName].State = 2
		for _, filename := range args.FileNames {
			index := strings.LastIndex(filename, "-")
			num, _ := strconv.Atoi(filename[index+1:])
			if v, ok := m.reduceTask[num]; ok {
				v.IntermediateFileNames = append(v.IntermediateFileNames, filename)
			} else {
				task := &Task{
					TaskType:              2,
					IntermediateFileNames: []string{filename},
					Taskname:              num,
				}
				m.reduceTask[num] = task
			}
		}
		m.mapTaskFinishNum++
		if m.mapTaskFinishNum == len(m.mapTask) {
			m.state = 1
		}
	} else {
		// late's task
		if m.reduceTask[args.TaskName].State == 2 {
			return nil
		}
		taskName := args.TaskName
		m.reduceTask[taskName].State = 2
		m.reduceTaskFinishNum++
		if m.reduceTaskFinishNum == len(m.reduceTask) {
			m.state = 2
		}
	}
	return nil
}

func (m *Master) Check(taskType int, taskName int) {
	time.Sleep(10*time.Second)
	if taskType==1{
		if m.mapTask[taskName].State==1{
			m.mapTask[taskName].State = 0
		}
	}else{
		if m.reduceTask[taskName].State==1{
			m.reduceTask[taskName].State=0
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	if m.state == 2 {
		ret = true
	}
	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		state:      0,
		nReduce:    nReduce,
		nMap:       40, //假设开4个worker
		mapTask:    []*Task{},
		reduceTask: map[int]*Task{},
	}

	// Your code here.
	// init map task
	for i, filename := range files {
		newTask := &Task{
			Taskname:      i,
			TaskType:      1,
			State:         0,
			InputFileName: filename,
			NReduce:       nReduce,
		}
		m.mapTask = append(m.mapTask, newTask)
	}

	m.server()
	return &m
}
