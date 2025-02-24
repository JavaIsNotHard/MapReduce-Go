package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

var nReduce int

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

    response, err := RequestTask()
    if err != nil {
        log.Print(err)
        os.Exit(1)
    }

    filename := response.FileName
    mapId := response.TaskId
    nReduce = response.ReduceCount

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

    OutputMapTask(kva, mapId)
}

func OutputMapTask(kva []KeyValue, mapId int) {
    prefix := fmt.Sprintf("mr-%v", mapId)
    // placeholder for all the files that is equal to the reduce task number
    files := make([]*os.File, 0, nReduce)
    // encoder to encode those n files
    encoders := make([]*json.Encoder, 0, nReduce)

    for i := 0; i < nReduce; i++ {
        filename := fmt.Sprintf("%v-%v", prefix, i)
        file, err := os.Create(filename)
        if err != nil {
            log.Print(err)
            os.Exit(1)
        }

        defer file.Close()
        files = append(files, file)
        encoders = append(encoders, json.NewEncoder(file))
    }

    for _, kv := range kva {
        index := ihash(kv.Key) % nReduce
        err := encoders[index].Encode(&kv)
        if err != nil {
            log.Print("Couldn't encode the file content into json")
            os.Exit(1)
        }
    }

}

func RequestTask() (*TaskRequestResponse, error) {
    request := TaskRequestArgs{os.Getpid()}
    response := TaskRequestResponse{}

    ok := call("Coordinator.ReturnTask", &request, &response)
    if ok {
        return &response, nil
    } else {
        return nil, fmt.Errorf("Couldn't call remote procedure call")
    }
}

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

	fmt.Println(err)
	return false
}
