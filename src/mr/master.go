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
	TaskTimeout      = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

const (
	TaskStatusReady = iota
	TaskStatusQueue
	TaskStatusRunning
	TaskStatusFinished
	TaskStatusErr
)

type Master struct {
	// Your definitions here.
	TaskCh    chan *Task
	Files     []string
	NMap      int
	NReduce   int
	NWorker   int
	Hasdone   bool
	TaskPhase TaskPhase
	TaskStats []TaskStat
	Mu        sync.Mutex
}

type TaskStat struct {
	Status    int
	WorkerID  int
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) initMapTasks() {
	m.TaskPhase = MapPhase
	m.TaskStats = make([]TaskStat, m.NMap)
}

func (m *Master) initReduceTasks() {
	m.TaskPhase = ReducePhase
	m.TaskStats = make([]TaskStat, m.NReduce)
}

func (m *Master) makeTask(taskID int) *Task {
	task := Task{
		FileName: "",
		NReduce:  m.NReduce,
		NMap:     m.NMap,
		TaskID:   taskID,
		Phase:    m.TaskPhase,
	}
	if task.Phase == MapPhase {
		task.FileName = m.Files[taskID]
	}
	return &task
}

// 周期性的调度，处理一些任务处理超时的问题
func (m *Master) tickSchedule() {
	for !m.Hasdone {
		m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

func (m *Master) schedule() {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	if m.Hasdone {
		return
	}

	allDone := true
	for i, t := range m.TaskStats {
		switch t.Status {
		case TaskStatusReady:
			allDone = false
			m.TaskCh <- m.makeTask(i)
			m.TaskStats[i].Status = TaskStatusQueue
		case TaskStatusQueue:
			allDone = false
		case TaskStatusRunning:
			allDone = false
			if time.Now().Sub(t.StartTime) > TaskTimeout {
				m.TaskStats[i].Status = TaskStatusQueue
				m.TaskCh <- m.makeTask(i)
			}
		case TaskStatusFinished:
		case TaskStatusErr:
			allDone = false
			m.TaskStats[i].Status = TaskStatusQueue
			m.TaskCh <- m.makeTask(i)
		default:
			panic("t.Status undefined")
		}
	}

	if allDone {
		if m.TaskPhase == MapPhase {
			m.initReduceTasks()
		} else {
			m.Hasdone = true
		}
	}
}

func (m *Master) registerTask(args *GetTaskArgs, task *Task) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.TaskStats[task.TaskID].Status = TaskStatusRunning
	m.TaskStats[task.TaskID].WorkerID = args.WorkerID
	m.TaskStats[task.TaskID].StartTime = time.Now()
}

// RPC handler, Get a map/reduce task for workers
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task := <-m.TaskCh
	reply.Task = task
	m.registerTask(args, task)
	return nil
}

// RPC handler. Register a worker, reply a WorkerID
func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	reply.WorkerID = m.NWorker
	m.NWorker++
	return nil
}

// RPC handler. Report task Done or Error and then schedule
func (m *Master) ReportTask(args *ReportTaskArgs, reply *RegisterWorkerReply) error {
	m.Mu.Lock()

	// a map task may run more than one time cause redudant running
	if args.TaskPhase != m.TaskPhase || m.TaskStats[args.TaskID].WorkerID != args.WorkerID {
		m.Mu.Unlock()
		return nil
	}
	if args.Done {
		m.TaskStats[args.TaskID].Status = TaskStatusFinished
	} else {
		m.TaskStats[args.TaskID].Status = TaskStatusErr
	}
	m.Mu.Unlock()
	m.schedule()
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

	// Your code here.
	return m.Hasdone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.Files = files
	m.NMap = len(files)
	m.NReduce = nReduce
	if m.NReduce > m.NMap {
		m.TaskCh = make(chan *Task, m.NReduce)
	} else {
		m.TaskCh = make(chan *Task, m.NMap)
	}
	m.initMapTasks()
	m.server()
	go m.tickSchedule()
	return &m
}
