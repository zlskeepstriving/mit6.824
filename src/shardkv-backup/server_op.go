package shardkv

import (
	"fmt"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string
	ClientId  int64
	MsgId     int64
	ReqId     int64
	ConfigNum int
}

type NotifyMsg struct {
	Err   Err
	Value string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	defer kv.log(fmt.Sprintf("rpc get: args: %+v, reply: %+v", args, reply))

	op := Op{
		Key:       args.Key,
		Op:        "Get",
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ReqId:     nrand(),
		ConfigNum: args.ConfigNum,
	}

	res := kv.waitCmd(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	defer kv.log(fmt.Sprintf("rpc putAppend: args: %+v, reply: %+v", args, reply))

	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		ReqId:     nrand(),
		ConfigNum: args.ConfigNum,
	}
	reply.Err = kv.waitCmd(op).Err
}

func (kv *ShardKV) removeCh(reqId int64) {
	kv.lock("removeCh")
	delete(kv.notifyCh, reqId)
	kv.unlock("removeCh")
}

func (kv *ShardKV) waitCmd(op Op) (res NotifyMsg) {
	kv.lock("waitCmd")
	ch := make(chan NotifyMsg, 1)

	if op.ConfigNum == 0 || op.ConfigNum < kv.config.Num {
		kv.log("ConfigReadyErr1")
		res.Err = ErrWrongGroup
		kv.unlock("waitCmd")
		return
	}
	kv.unlock("waitCmd")

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	kv.lock("waitCmd")
	kv.notifyCh[op.ReqId] = ch
	kv.unlock("waitCmd")

	kv.log(fmt.Sprintf("start cmd: index %d, term: %d, op: %+v", index, term, op))
	t := time.NewTimer(WaitCmdTimeout)
	defer t.Stop()

	select {
	case res = <-ch:
		kv.removeCh(op.ReqId)
		return
	case <-t.C:
		kv.removeCh(op.ReqId)
		res.Err = ErrTimeout
		return
	}
}
