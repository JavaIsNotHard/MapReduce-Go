package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskStatus int
type TaskType int

const (
    NotStarted TaskStatus = iota 
    Executing 
    Finished
)

const (
    MapTask TaskType = iota 
    ReduceTask 
)

type Task struct {
    Type TaskType
    Status TaskStatus
    FileName string 
    Index int
    WorkerId int
}


type Coordinator struct {
    MapTasks []Task
    MapTaskNumber int
    ReduceTaskNumber int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) ReturnTask(args *TaskRequestArgs, reply *TaskRequestResponse) error {
    for i := 0; i < c.MapTaskNumber; i++ {
        if c.MapTasks[i].Status == NotStarted {
            c.MapTasks[i].Status = Executing
            c.MapTasks[i].WorkerId = args.WorkerId

            reply.FileName = c.MapTasks[i].FileName
            reply.WorkerId = c.MapTasks[i].WorkerId
            reply.TaskId = c.MapTasks[i].Index
            reply.ReduceCount = c.ReduceTaskNumber
            return nil
        }
    }

    return fmt.Errorf("No task to start")
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
    nMap := len(files)
    
    c := Coordinator{}

    c.MapTaskNumber = nMap
    c.ReduceTaskNumber = nReduce
    c.MapTasks = make([]Task, 0, nMap)

    for i := 0; i < nMap; i++ {
        mapTask := Task{MapTask, NotStarted, files[i], i, -1}
        c.MapTasks = append(c.MapTasks, mapTask)
    }

	c.server()
	return &c
}
