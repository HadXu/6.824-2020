package mr

import (
	"os"
	"strconv"
)

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type AcquireTaskRequest struct {
}

type MapTask struct {
	ID      int
	NReduce int
	File    string
}

type ReduceTask struct {
	ID int
}

type AcquireTaskReply struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
	Allfinish  bool
}

type TaskStateRequest struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
	Error      error
}

type TaskStateReply struct {
}

func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
