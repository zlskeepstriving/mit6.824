package mr

import (
	"fmt"
)

type TaskPhase int
type TaskStatus int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	FileName string
	NReduce  int
	NMap     int
	TaskID   int
	Phase    TaskPhase
}

func IntermediateName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func OutputName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
