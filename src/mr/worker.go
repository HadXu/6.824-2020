package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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

func RegisterMyself() int {
	args := RegisterArgs{}
	reply := RegisterReply{}

	call("Master.Register", &args, &reply)
	fmt.Printf("My worker id is %v\n", reply.No)
	return reply.No
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 注册worker
	id := RegisterMyself()

	for {
		args := ScheduleTaskArgs{}
		args.Id = id
		reply := ScheduleTaskReply{}
		call("Master.ScheduleTask", &args, &reply)

		if reply.Retcode == NO_MORE_TASK {
			fmt.Printf("Worker %v no more task\n", id)
			break
		}
		if reply.Retcode == SUCCESS {
			switch reply.Task {
			case TASK_MAP:
				fmt.Printf("Worker %v DoMap on %v\n", id, reply.File)
				DoMap(id, reply.File, reply.NReduce, mapf)

			case TASK_REDUCE:
				fmt.Printf("Worker %v DoReduce on %v\n", id, reply.File)
				DoReduce(id, reply.File, reply.NReduce, reducef)
			}
		}
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func DoMap(id int, filename string, nslot int, mapf func(string, string) []KeyValue) {
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

	var fws []*os.File
	for i := 0; i < nslot; i++ {
		target := "/Users/haxu/mygo/src/main/mr-tmp/mr-" + filepath.Base(filename) + "-" + strconv.Itoa(i)
		file, _ := os.Create(target)
		fws = append(fws, file)
	}
	for _, kv := range kva {
		enc := json.NewEncoder(fws[ihash(kv.Key)%nslot])
		enc.Encode(&kv)
	}

	for i := 0; i < nslot; i++ {
		fws[i].Close()
	}

}

func DoReduce(id int, filename string, nslot int, reducef func(string, []string) string) {
	var files []string
	root := "/Users/haxu/mygo/src/main/mr-tmp"
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	kvm := make(map[string][]string)
	for _, sub := range files {
		matched, err := filepath.Match(filepath.Join(root, filename), sub)
		if err != nil {
			log.Fatal(err)
		}
		if matched {
			kva := make([]KeyValue, 0)
			file, err := os.OpenFile(sub, os.O_RDONLY, 0644)
			if err != nil {
				log.Fatal(err)
			}
			defer file.Close()
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}

			for _, kv := range kva {
				if _, ok := kvm[kv.Key]; !ok {
					kvm[kv.Key] = make([]string, 0)
				}
				kvm[kv.Key] = append(kvm[kv.Key], kv.Value)
			}
		}
	}

	file, err := os.OpenFile("mr-out-"+strconv.Itoa(nslot), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	defer file.Close()

	for k, v := range kvm {
		_, err = file.WriteString(k + " " + reducef(k, v) + "\n")
	}
}

//
// example function to show how to make an RPC call to the master.
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
