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

const (
	taskUnstarted int = iota
	taskReserved
	taskFinished
)

type mapTask struct {
	file      string
	taskID    int
	state     int
	startTime time.Time
}
type reduceTask struct {
	taskID    int
	state     int
	startTime time.Time
}

type Master struct {
	// Your definitions here.
	sync.Mutex
	mapFinished   bool
	mapTasks      []mapTask
	reduceTasks   []reduceTask
	nReduce       int
	done          bool
	stagingReduce bool // ?
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AcquireTask(req *AcquireTaskRequest, reply *AcquireTaskReply) error {
	m.Lock()
	defer m.Unlock()

	if m.done {
		reply.Allfinish = true
		return nil
	}

	if !m.mapFinished {
		var task *mapTask
		m.mapFinished = true
		func() {
			for i := range m.mapTasks {
				switch m.mapTasks[i].state {
				case taskUnstarted:
					m.mapTasks[i].state = taskReserved
					m.mapTasks[i].startTime = time.Now()
					task = &m.mapTasks[i]
					m.mapFinished = false
					//如果遇到一个没有完成 就返回给worker去完成
					return
				case taskReserved:
					m.mapFinished = false
				}
			}
		}()
		if task != nil {
			reply.MapTask = &MapTask{
				ID:      task.taskID,
				NReduce: m.nReduce,
				File:    task.file,
			}
		}

	} else {
		var task *reduceTask
		func() {
			for i := range m.reduceTasks {
				switch m.reduceTasks[i].state {
				case taskUnstarted:
					m.reduceTasks[i].state = taskReserved
					m.reduceTasks[i].startTime = time.Now()
					task = &m.reduceTasks[i]
					return
				}
			}
		}()
		if task != nil {
			reply.ReduceTask = &ReduceTask{
				ID: task.taskID,
			}
		}
	}

	return nil
}

func (m *Master) TaskState(args *TaskStateRequest, reply *TaskStateReply) error {
	m.Lock()
	defer m.Unlock()
	if args.MapTask != nil {
		if args.Error == nil {
			m.mapTasks[args.MapTask.ID].state = taskFinished
		} else {
			m.mapTasks[args.MapTask.ID].state = taskUnstarted
		}
	}

	if args.ReduceTask != nil {
		if args.Error == nil {
			m.reduceTasks[args.ReduceTask.ID].state = taskFinished
		} else {
			m.reduceTasks[args.ReduceTask.ID].state = taskUnstarted
		}
	}

	// for i := range m.mapTasks {
	// 	fmt.Printf("%v ", m.mapTasks[i].state)
	// }

	// for i := range m.mapTasks {
	// 	fmt.Printf("%v ", m.reduceTasks[i].state)
	// }
	// fmt.Println()
	return nil
}

//
// an example RPC handler.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	return m.done
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	fileCount := len(files)
	mapTasks := make([]mapTask, len(files))
	reduceTasks := make([]reduceTask, nReduce)
	for i := 0; i < fileCount; i++ {
		mapTasks[i] = mapTask{
			file:   files[i],
			taskID: i,
			state:  taskUnstarted,
		}
	}

	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = reduceTask{
			taskID: i,
			state:  taskUnstarted,
		}
	}
	m.mapTasks = mapTasks
	m.reduceTasks = reduceTasks
	m.nReduce = nReduce
	//开一个线程实时监控master的状态
	go func() {
		for {
			finish := true
			now := time.Now()
			m.Lock()
			for i := range m.mapTasks {
				if m.mapTasks[i].state != taskFinished {
					finish = false
				}
				if m.mapTasks[i].state == taskReserved && now.Sub(m.mapTasks[i].startTime) > 10*time.Second {
					m.mapTasks[i].state = taskUnstarted
				}
			}

			for i := range m.reduceTasks {
				if m.reduceTasks[i].state != taskFinished {
					finish = false
				}
				if m.reduceTasks[i].state == taskReserved && now.Sub(m.reduceTasks[i].startTime) > 10*time.Second {
					m.reduceTasks[i].state = taskUnstarted
				}
			}
			m.Unlock()
			if finish {
				m.done = true
			}
			time.Sleep(time.Second)
		}
	}()
	m.server()
	return &m
}
