package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu             sync.Mutex
	latestLeaderId int //save latest Leader's id so that client doesn't need to traverse all server
	clientId       int64
	opSeq          int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.opSeq = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	ck.opSeq += 1
	opSeq := ck.opSeq
	clientId := ck.clientId
	ck.mu.Unlock()

	needLoop := true
	nextSendServerId := ck.latestLeaderId
	args := GetArgs{
		Key:      key,
		OpName:   "Get",
		ClientId: clientId,
		Seq:      opSeq,
	}
	for needLoop {
		reply := GetReply{}
		if ok := ck.servers[nextSendServerId].Call("KVServer.Get", &args, &reply); ok {
			if reply.Err == ErrWrongLeader {
				nextSendServerId = (nextSendServerId + 1) % len(ck.servers)
				continue
			} else {
				DPrintf("client receive RPC response from Leader peer[%d], reply[%v]", nextSendServerId, reply)
				ck.mu.Lock()
				ck.latestLeaderId = nextSendServerId
				ck.mu.Unlock()
				if reply.Err == OK {
					return reply.Value
				} else if reply.Err == ErrNoKey {
					return ""
				}
				needLoop = false
				break
			}
		} else {
			nextSendServerId = (nextSendServerId + 1) % len(ck.servers)
			continue
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.opSeq += 1
	opSeq := ck.opSeq
	clientId := ck.clientId
	ck.mu.Unlock()

	needLoop := true
	nextSendServerId := ck.latestLeaderId
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		OpName:   op,
		ClientId: clientId,
		Seq:      opSeq,
	}
	for needLoop {
		reply := PutAppendReply{}
		if ok := ck.servers[nextSendServerId].Call("KVServer.PutAppend", &args, &reply); ok {
			if reply.Err == ErrWrongLeader {
				nextSendServerId = (nextSendServerId + 1) % len(ck.servers)
				continue
			} else if reply.Err == OK {
				DPrintf("client receive RPC response from peer[%d], reply[%v]", nextSendServerId, reply)
				ck.mu.Lock()
				ck.latestLeaderId = nextSendServerId
				ck.mu.Unlock()
				needLoop = false
				return
			}
		} else {
			nextSendServerId = (nextSendServerId + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
