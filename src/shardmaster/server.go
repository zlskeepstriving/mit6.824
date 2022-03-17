package shardmaster

import (
	"bytes"
	"log"
	"reflect"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const (
	Debug             = false
	CONSENSUS_TIMEOUT = 5000 //ms
	QueryOp           = "query"
	JoinOp            = "join"
	LeaveOp           = "leave"
	MoveOp            = "move"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	maxraftstate int
	applyCh      chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	waitApplyCh   map[int]chan Op // index(raft) -> chan
	lastRequestId map[int64]int   // clientId -> requestId

}

type Op struct {
	// Your data here.
	Operation    string
	ClientId     int64
	RequestId    int
	Num_Query    int
	Servers_Join map[int][]string
	Gids_Leave   []int
	Shard_Move   int
	Gid_Move     int
}

func (sm *ShardMaster) runOp(op Op) string {
	DPrintf("server [%d] start op[%v]", sm.me, op)
	opIndex, _, _ := sm.rf.Start(op)

	//create WaitForCh
	sm.mu.Lock()
	opIndexCh, exist := sm.waitApplyCh[opIndex]
	if !exist {
		sm.waitApplyCh[opIndex] = make(chan Op, 1)
		opIndexCh = sm.waitApplyCh[opIndex]
	}
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		delete(sm.waitApplyCh, opIndex)
		sm.mu.Unlock()
	}()

	//wait raft to reach agreement
	select {
	case commitOp := <-opIndexCh:
		if reflect.DeepEqual(commitOp, op) {
			return OK
		} else {
			return ErrWrongLeader
		}
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		return ErrWrongLeader
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("--------server[%d] Join Info----------", sm.me)
	for gid, servers := range args.Servers {
		DPrintf("[Gid]==%d", gid)
		DPrintf("[servers]%v", servers)
	}

	op := Op{
		Operation:    JoinOp,
		ClientId:     args.ClientId,
		RequestId:    args.RequestId,
		Servers_Join: args.Servers,
	}

	result := sm.runOp(op)
	if result != OK {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation:  LeaveOp,
		ClientId:   args.ClientId,
		RequestId:  args.RequestId,
		Gids_Leave: args.GIDs,
	}

	result := sm.runOp(op)
	if result != OK {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
		reply.WrongLeader = false
	}

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation:  MoveOp,
		ClientId:   args.ClientId,
		RequestId:  args.RequestId,
		Shard_Move: args.Shard,
		Gid_Move:   args.GID,
	}

	result := sm.runOp(op)
	if result != OK {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation: QueryOp,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Num_Query: args.Num,
	}

	result := sm.runOp(op)
	if result != OK {
		reply.Err = ErrWrongLeader
	} else {
		configIndex := op.Num_Query
		sm.mu.Lock()
		if configIndex < 0 || configIndex >= len(sm.configs) {
			configIndex = len(sm.configs) - 1
		}
		reply.Config = sm.configs[configIndex]
		DPrintf("shards:%v", sm.configs[configIndex].Shards)
		sm.mu.Unlock()
		reply.Err = OK
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) applyLoop() {
	for {
		applyLog := <-sm.applyCh
		if !applyLog.CommandValid { //snapshot
			sm.GetSnapShotFromRaft(applyLog)
		} else {
			sm.GetCommandFromRaft(applyLog)
		}
	}
}

//related to snapshot
func (sm *ShardMaster) needSnapshot(index int) bool {
	return sm.maxraftstate != -1 && sm.rf.PersisterSize() >= sm.maxraftstate*9/10
}

func (sm *ShardMaster) GetSnapShotFromRaft(applyLog raft.ApplyMsg) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.ReadSnapShotToInstall(applyLog.Command.([]byte))
}

func (sm *ShardMaster) MakeSnapshot() []byte {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sm.configs)
	e.Encode(sm.lastRequestId)
	return w.Bytes()
}

func (sm *ShardMaster) ReadSnapShotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_config []Config
	var persist_lastRequestId map[int64]int

	if d.Decode(&persist_config) != nil || d.Decode(&persist_lastRequestId) != nil {
		DPrintf("KVSERVER %d read persister got a problem!!!!!!", sm.me)
	} else {
		sm.configs = persist_config
		sm.lastRequestId = persist_lastRequestId
	}
}

//related to apply command
func (sm *ShardMaster) GetCommandFromRaft(applyLog raft.ApplyMsg) {
	op := applyLog.Command.(Op)

	sm.mu.Lock()
	applyIndex := applyLog.CommandIndex
	op, ok := applyLog.Command.(Op)
	if !ok {
		log.Fatal("[applyLoop]: interface asert failed")
	}

	//detect duplicate request
	//if op.RequestId > sm.lastRequestId[op.ClientId] {}
	sm.applyState(op)
	//sm.lastRequestId[op.ClientId] = op.RequestId
	if opIndexCh, ok := sm.waitApplyCh[applyIndex]; ok {
		//if there exist op in Opch, take it out
		select {
		case <-opIndexCh:
		default:
		}
		//wake up RPC handler(runOp)
		opIndexCh <- op
	}
	if sm.needSnapshot(applyIndex) {
		snapshot := sm.MakeSnapshot()
		go sm.rf.SnapShot(applyIndex, snapshot)
	}
	sm.mu.Unlock()
}

func (sm *ShardMaster) applyState(op Op) {
	DPrintf("server[%d] apply op[%v]", sm.me, op)
	switch op.Operation {
	case JoinOp:
		sm.runJoinOp(op)
	case LeaveOp:
		sm.runLeaveOp(op)
	case MoveOp:
		sm.runMoveOp(op)
	default:
	}
}

func (sm *ShardMaster) runJoinOp(joinOp Op) {
	config := sm.configs[len(sm.configs)-1]
	config.Num = len(sm.configs)
	config.Groups = make(map[int][]string)
	//deep copy
	for gid, servers := range sm.configs[len(sm.configs)-1].Groups {
		config.Groups[gid] = servers
	}
	// new servers join
	for gid, servers := range joinOp.Servers_Join {
		config.Groups[gid] = append(config.Groups[gid], servers...)
	}
	sm.reBalance(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) runLeaveOp(leaveOp Op) {
	config := sm.configs[len(sm.configs)-1]
	config.Num = len(sm.configs)
	config.Groups = make(map[int][]string)
	for gid, servers := range sm.configs[len(sm.configs)-1].Groups {
		config.Groups[gid] = servers
	}
	//groups leave
	for _, gid := range leaveOp.Gids_Leave {
		delete(config.Groups, gid)
		for shardId, oldGid := range config.Shards {
			if oldGid == gid {
				config.Shards[shardId] = 0
			}
		}
	}
	sm.reBalance(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) runMoveOp(moveOp Op) {
	config := sm.configs[len(sm.configs)-1]
	config.Num = len(sm.configs)
	config.Groups = make(map[int][]string)
	for gid, servers := range sm.configs[len(sm.configs)-1].Groups {
		config.Groups[gid] = servers
	}
	//move shard from oldGroup to newGroup
	config.Shards[moveOp.Shard_Move] = moveOp.Gid_Move
	sm.reBalance(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) reBalance(config *Config) {
	groupNum := make(map[int][]int) // shard num of group
	for shardId, gid := range config.Shards {
		if groupNum[gid] == nil {
			groupNum[gid] = []int{}
		}
		groupNum[gid] = append(groupNum[gid], shardId)
	}
	for gid := range config.Groups {
		if groupNum[gid] == nil {
			groupNum[gid] = []int{}
		}
	}
	DPrintf("groupNum: %v", groupNum)
	for {
		shardId, gid, isBalance := sm.getBalanceGidAndShard(&groupNum)
		if isBalance {
			break
		}
		oldGroup := config.Shards[shardId]
		groupNum[oldGroup] = groupNum[oldGroup][0 : len(groupNum[oldGroup])-1]
		groupNum[gid] = append(groupNum[gid], shardId)
		config.Shards[shardId] = gid
	}
}

func (sm *ShardMaster) getBalanceGidAndShard(groupNum *map[int][]int) (int, int, bool) {
	shardId := 0
	min_num := 0x3f3f3f3f
	max_num := 0
	min_num_gid := 0
	for gid, shards := range *groupNum {
		if gid == 0 && len(shards) > 0 { //第0组有分片时，直接将最后一个shard拿出作为要迁移的shard
			max_num = 0x3f3f3f3f
			shardId = shards[len(shards)-1]
		}
		if len(shards) > max_num {
			max_num = len(shards)
			shardId = shards[len(shards)-1]
		}
		if gid > 0 && len(shards) < min_num {
			min_num = len(shards)
			min_num_gid = gid
		}
	}
	DPrintf("max_num [%d], min_num [%d]", max_num, min_num)
	isBalance := max_num-min_num <= 1
	return shardId, min_num_gid, isBalance
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.maxraftstate = -1

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.waitApplyCh = make(map[int]chan Op)
	sm.lastRequestId = make(map[int64]int)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		sm.mu.Lock()
		sm.ReadSnapShotToInstall(snapshot)
		sm.mu.Unlock()
	}
	go sm.applyLoop()

	return sm
}
