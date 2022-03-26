package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type MRWorker struct {
	ID      int
	Mapf    func(string, string) []KeyValue
	Reducef func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func (w *MRWorker) runMapTask(task *Task) {
	content, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		w.CallReportTask(task, err)
		return
	}
	kvs := w.Mapf(task.FileName, string(content))
	intermediate := make([][]KeyValue, task.NReduce, task.NReduce)
	for _, kv := range kvs {
		index := ihash(kv.Key) % task.NReduce
		intermediate[index] = append(intermediate[index], kv)
	}
	for reduceIdx := 0; reduceIdx < task.NReduce; reduceIdx++ {
		f, err := os.Create(IntermediateName(task.TaskID, reduceIdx))
		if err != nil {
			w.CallReportTask(task, err)
			return
		}
		data, _ := json.Marshal(intermediate[reduceIdx])
		_, err = f.Write(data)
		if err != nil {
			w.CallReportTask(task, err)
			return
		}
		err = f.Close()
		if err != nil {
			w.CallReportTask(task, err)
			return
		}
	}
	//fmt.Printf("intermediate map file %v has been generated successfully\n", oname)
	w.CallReportTask(task, nil)
}

func (w *MRWorker) runReduceTask(task *Task) {
	kvReduce := make(map[string][]string)
	for mapIdx := 0; mapIdx < task.NMap; mapIdx++ {
		content, err := ioutil.ReadFile(IntermediateName(mapIdx, task.TaskID))
		if err != nil {
			w.CallReportTask(task, err)
			return
		}
		kvs := make([]KeyValue, 0)
		err = json.Unmarshal(content, &kvs)
		if err != nil {
			w.CallReportTask(task, err)
			return
		}
		for _, kv := range kvs {
			_, ok := kvReduce[kv.Key]
			if !ok {
				kvReduce[kv.Key] = make([]string, 0, 100)
			}
			kvReduce[kv.Key] = append(kvReduce[kv.Key], kv.Value)
		}
	}
	data := make([]string, 0, 100)
	for k, v := range kvReduce {
		data = append(data, fmt.Sprintf("%v %v\n", k, w.Reducef(k, v)))
	}
	err := ioutil.WriteFile(OutputName(task.TaskID), []byte(strings.Join(data, "")), 0600)
	if err != nil {
		w.CallReportTask(task, err)
		return
	}
	w.CallReportTask(task, nil)
	//fmt.Printf("mr-out %v file has been generated successfully\n", outputFileName)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := MRWorker{
		Mapf:    mapf,
		Reducef: reducef,
	}
	w.CallRegisterWorker()
	w.run()
}

func (w *MRWorker) run() {
	for {
		t := w.CallGetTask()
		switch t.Phase {
		case MapPhase:
			w.runMapTask(t)
		case ReducePhase:
			w.runReduceTask(t)
		default:
			panic("t.Phase undefined\n")
		}
	}
}

func (w *MRWorker) CallRegisterWorker() {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	if !call("Master.RegisterWorker", &args, &reply) {
		log.Fatal("Register failed!\n")
	}
	w.ID = reply.WorkerID
}

func (w *MRWorker) CallGetTask() *Task {
	args := GetTaskArgs{w.ID}
	reply := GetTaskReply{}
	if !call("Master.GetTask", &args, &reply) {
		log.Fatal("GetTask failed, worker process exit!\n")
	}
	return reply.Task
}

func (w *MRWorker) CallReportTask(t *Task, err error) {
	args := ReportTaskArgs{
		Done:      true,
		TaskID:    t.TaskID,
		WorkerID:  w.ID,
		TaskPhase: t.Phase,
	}
	if err != nil {
		args.Done = false
		log.Println(args, err)
	}
	reply := ReportTaskReply{}
	if !call("Master.ReportTask", &args, &reply) {
		log.Println("ReportTask failed", args)
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
