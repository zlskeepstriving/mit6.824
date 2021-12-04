package shardmaster

import (
	"sync"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
	}
	newConfig := new(Config)
	sm.mu.Lock()
	lastConfigNum := len(sm.configs) - 1
	newConfig.Num = sm.configs[lastConfigNum].Num + 1
	oldGroups := sm.configs[lastConfigNum].Groups
	newGroups := make(map[int][]string, len(oldGroups))
	groupNum := 0
	for k, v := range oldGroups {
		if len(v) > 0 {
			newGroups[k] = v
			groupNum++
		}
	}
	for k, v := range args.Servers {
		if len(v) > 0 {
			newGroups[k] = v
			groupNum++
		}
	}

	// assign group for shards
	if groupNum <= NShards {
		avg := NShards / groupNum
		remain := NShards % groupNum
		i := 0
		groupNumList := make([]int, groupNum)
		for gid := range newGroups {
			groupNumList = append(groupNumList, gid)
		}
		for gid := range groupNumList {
			for j := avg; j > 0 && i < NShards; j++ {
				newConfig.Shards[i] = gid
				i++
			}
		}
		if remain > 0 {
			for gid := range newGroups {
				newConfig.Shards[i] = gid
				i++
				remain--
				if remain == 0 {
					break
				}
			}
		}
	} else {
		i := 0
		for gid := range newGroups {
			newConfig.Shards[i] = gid
			i++
			if i == NShards {
				break
			}
		}
	}
	newConfig.Groups = newGroups
	sm.mu.Unlock()

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
	}

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
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
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	return sm
}
