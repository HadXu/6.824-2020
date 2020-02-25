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

// 注册worker
type RegisterArgs struct {
}
type RegisterReply struct {
	No int
}

// 分配worker任务
type ScheduleTaskArgs struct {
	Id int
}
type ScheduleTaskReply struct {
	Retcode ReturnCode
	Task    WorkerTask
	File    string
	NReduce int
}

type WorkerTask int

const (
	TASK_NONE   WorkerTask = 0
	TASK_MAP    WorkerTask = 1
	TASK_REDUCE WorkerTask = 2
)

type WorkerState int

const (
	WORKER_IDLE              WorkerState = 0
	WORKER_MAP_INPROGRESS    WorkerState = 1
	WORKER_COMPLETED         WorkerState = 2
	WORKER_REDUCE_INPROGRESS WorkerState = 3
)

type ReturnCode int

const (
	SUCCESS       ReturnCode = 0
	NO_MORE_TASK  ReturnCode = 1
	WAIT_FOR_TASK ReturnCode = 2
)

func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
