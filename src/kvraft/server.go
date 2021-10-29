package kvraft

import (
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpName   string
	Seq      int
	ClientId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine    map[string]string
	latestClientSeq map[int64]int   //record the latest op seq for each client
	notifyCh        map[int]chan Op //store command based on log index
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//DPrintf("server[%d] identity[%s], call Get, args[%v], current kv[%v]", kv.me, kv.rf.GetIdentity(), args, kv.KvData)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Key:      args.Key,
		Value:    "this is a Get() operation",
		OpName:   args.OpName,
		Seq:      args.Seq,
		ClientId: args.ClientId,
	}

	result := kv.runOp(op)
	if result != OK {
		reply.Err = ErrWrongLeader
		return
	} else {
		kv.mu.Lock()
		val, ok := kv.stateMachine[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = val
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintf("server[%d] call PutAppend, args[%v], current kv[%v]", kv.me, args, kv.KvData)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		OpName:   args.OpName,
		Seq:      args.Seq,
		ClientId: args.ClientId,
	}

	result := kv.runOp(op)
	if result != OK {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
}

func (kv *KVServer) runOp(op Op) string {
	DPrintf("server [%d] start op[%v]", kv.me, op)
	index, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	opCh, ok := kv.notifyCh[index]
	if !ok {
		opCh = make(chan Op)
		kv.notifyCh[index] = opCh
	}
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()

	//wait raft to reach agreement, receive the op of index which from Start()'s return
	select {
	case logOp := <-opCh:
		if reflect.DeepEqual(logOp, op) {
			return OK
		} else {
			return ErrWrongLeader
		}
	case <-time.After(2 * time.Second):
		return ErrTimeout
	}
}

func (kv *KVServer) applyLoop() {
	for {
		applyLog := <-kv.applyCh
		op, ok := applyLog.Command.(Op)
		if !ok {
			log.Printf("[applyLoop]: error")
		}

		kv.mu.Lock()
		//detect duplicate request
		if op.Seq > kv.latestClientSeq[op.ClientId] {
			kv.applyState(op)
			kv.latestClientSeq[op.ClientId] = op.Seq
		}
		index := applyLog.CommandIndex
		if opCh, ok := kv.notifyCh[index]; ok {
			// if there exist op in opCh, take it out
			select {
			case <-opCh:
			default:
			}
			// wake up RPC handler
			opCh <- op
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) applyState(op Op) {
	switch op.OpName {
	case "Put":
		kv.stateMachine[op.Key] = op.Value
	case "Append":
		if _, ok := kv.stateMachine[op.Key]; !ok {
			kv.stateMachine[op.Key] = op.Value
		} else {
			kv.stateMachine[op.Key] += op.Value
		}
	default:
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.stateMachine = make(map[string]string)
	kv.latestClientSeq = make(map[int64]int)
	kv.notifyCh = make(map[int]chan Op)

	go kv.applyLoop()
	// You may need initialization code here.
	return kv
}
