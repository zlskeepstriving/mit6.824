package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	taskCh     chan Task
	files      []string
	nReduce    int
	taskPhase  TaskPhase
	taskStatus []TaskStat
	workerID   int
	fileToTask map[string]*Task
	mutex      sync.Mutex
	taskList   []*Task
	phase      TaskPhase
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

func (m *Master) MapTaskFinished(args *ExampleArgs, reply *MasterReply) error {
	m.mutex.Lock()
	//fmt.Println("rpc -----------------", args.TaskID)
	m.taskList[args.TaskID].Status = TaskStatusMapFinished
	m.mutex.Unlock()
	return nil
}

func (m *Master) ReduceTaskFinished(args *ExampleArgs, reply *MasterReply) error {
	m.mutex.Lock()
	m.taskList[args.TaskID].Status = TaskStatusFinished
	m.mutex.Unlock()
	return nil
}
func (m *Master) checkMapFinished() bool {
	var ret = true

	for _, task := range m.taskList {
		if task.Status != TaskStatusMapFinished {
			ret = false
			break
		}
	}

	return ret
}

func (m *Master) AssignTask(args *ExampleArgs, masterReply *MasterReply) error {
	masterReply.End = m.Done()
	if masterReply.End {
		return nil
	}

	m.mutex.Lock()
	if m.phase == MapPhase {
		for _, task := range m.taskList {
			if task.Status == TaskStatusReady {
				task.Phase = MapPhase
				masterReply.CurTask = *task
				task.Status = TaskStatusMapping
				//fmt.Println("Mapping file", task.MapFileName)
				break
			}
		}
		//check whether map phase has finished
		startReducePhase := m.checkMapFinished()
		if startReducePhase {
			m.phase = ReducePhase

			//update taskList for reduce phase
			var t int = len(m.taskList)
			for len(m.taskList) < m.nReduce {
				ts := &Task{
					Status: TaskStatusMapFinished,
					Phase:  ReducePhase,
					TaskID: t,
					NMaps:  m.taskList[0].NMaps,
				}
				t++
				m.taskList = append(m.taskList, ts)
			}
			fmt.Println("All MapTask have been finished, now start ReducePhase")
		}
	} else if m.phase == ReducePhase {
		for _, task := range m.taskList {
			if task.Status == TaskStatusMapFinished {
				task.Phase = ReducePhase
				masterReply.CurTask = *task
				task.Status = TaskStatusReducing
				//fmt.Printf("Reducing mr-*-%v\n", task.TaskID)
				break
			}
		}
	}
	m.mutex.Unlock()

	return nil
}

func (m *Master) WorkerCanExit(args *ExampleArgs, reply *MasterReply) error {
	reply.End = m.Done()
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
	ret := true

	// Your code here.

	m.mutex.Lock()
	for _, task := range m.taskList {
		//fmt.Println(task)
		if task.Status != TaskStatusFinished {
			ret = false
			break
		}
	}
	m.mutex.Unlock()
	if ret {
		fmt.Println("All Task have been finished successfully!")
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.files = files
	m.nReduce = nReduce
	var taskid int = 0
	for _, file := range m.files {
		task := Task{
			MapFileName: file,
			Status:      TaskStatusReady,
			TaskID:      taskid,
			NMaps:       len(files),
			NReduce:     nReduce,
		}
		taskid++
		m.taskList = append(m.taskList, &task)
		//fmt.Println(taskList)
	}
	m.phase = MapPhase
	// Your code here.

	m.server()
	return &m
}
