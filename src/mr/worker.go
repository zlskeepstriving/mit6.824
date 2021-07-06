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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func mapFunction(mapf func(string, string) []KeyValue, task *Task) {
	mapFilename := task.FileName
	file, err := os.Open(mapFilename)
	if err != nil {
		log.Fatalf("cannot open %v", mapFilename)
	}
	//fmt.Println("Start Mapping", mapFilename)
	content, _ := ioutil.ReadAll(file)
	file.Close()
	oname := "intermediate" + strconv.Itoa(task.TaskID)
	mapfile, _ := os.Create(oname)
	defer mapfile.Close()
	enc := json.NewEncoder(mapfile)
	intermediate := mapf(mapFilename, string(content))
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("map %v error\n", kv.Key)
		}
	}
	//fmt.Printf("intermediate map file %v has been generated successfully\n", oname)
	callMapTaskFinished(task)
}

func reduceFunction(reducef func(string, []string) string, task *Task) {
	interFileName := "intermediate" + strconv.Itoa(task.TaskID)
	reduceFile, err := os.Open(interFileName)
	if err == nil {
		//fmt.Println("Start Reducing ")
	}
	dec := json.NewDecoder(reduceFile)

	intermediate := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}

	outputFileName := "mr-out" + strconv.Itoa(task.TaskID)
	outputFile, _ := os.Create(outputFileName)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	//fmt.Printf("mr-out %v file has been generated successfully\n", outputFileName)
	callReduceTaskFinished(task)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		end := callEnd()
		if end {
			break
		}
		//time.Sleep(time.Second)
		// Your worker implementation here.
		task := callTask()
		if task.Phase == MapPhase {
			mapFunction(mapf, task)
		} else if task.Phase == ReducePhase {
			reduceFunction(reducef, task)
		}
	}
	fmt.Println("All Task have been finished, worker can exit")
	os.Exit(1)
}

func callTask() *Task {
	args := ExampleArgs{}
	masterReply := MasterReply{}
	if flag := call("Master.AssignTask", &args, &masterReply); flag {
		//fmt.Println("callTask is successful")
		return masterReply.CurTask
	}
	panic("call Task is fail")
}

func callEnd() bool {
	args := ExampleArgs{}
	masterReply := MasterReply{}
	if flag := call("Master.WorkerCanExit", &args, &masterReply); flag {
		//fmt.Println("callEnd is successful")
		return masterReply.End
	}
	panic("call end is fail")
}

func callMapTaskFinished(task *Task) {
	args := ExampleArgs{}
	args.TaskID = task.TaskID
	reply := MasterReply{}
	if call("Master.MapTaskFinished", &args, &reply) {
		//fmt.Println("call MapTaskFinished successfully.")
	}
}

func callReduceTaskFinished(task *Task) {
	args := ExampleArgs{}
	args.TaskID = task.TaskID
	reply := MasterReply{}
	if call("Master.ReduceTaskFinished", &args, &reply) {
		//fmt.Println("call ReduceTaskFinished successfully.")
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
