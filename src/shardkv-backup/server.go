package shardkv

// import "../shardmaster"
import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

const (
	PullConfigInterval       = time.Millisecond * 100
	PullShardsInterval       = time.Millisecond * 200
	WaitCmdTimeout           = time.Millisecond * 500
	ReqCleanShardDataTimeout = time.Millisecond * 500
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	config        shardmaster.Config
	oldConfig     shardmaster.Config
	notifyCh      map[int64]chan NotifyMsg
	lastMsgIdx    [shardmaster.NShards]map[int64]int64 // for detecting duplicate msg
	ownShards     map[int]bool
	data          [shardmaster.NShards]map[string]string
	waitShardIds  map[int]bool
	historyShards map[int]map[int]MergeShardData
	mck           *shardmaster.Clerk

	dead           int32
	stopCh         chan struct{}
	persister      *raft.Persister
	lastApplyIndex int
	lastApplyTerm  int

	pullConfigTimer *time.Timer
	pullShardsTimer *time.Timer

	DebugLog  bool
	lockStart time.Time // debug用,找出长时间的lock
	lockEnd   time.Time
	lockName  string
}

func (kv *ShardKV) lock(message string) {
	kv.mu.Lock()
	kv.lockName = message
	kv.lockStart = time.Now()
}

func (kv *ShardKV) unlock(message string) {
	kv.lockEnd = time.Now()
	d := kv.lockEnd.Sub(kv.lockStart)
	kv.mu.Unlock()
	if d > time.Millisecond*2 {
		kv.log(fmt.Sprintf("lock too long: %s, %s\n", message, d))
	}
}

func (kv *ShardKV) log(message string) {
	if kv.DebugLog {
		log.Printf("server [%d], gid [%d], config[%+v], waitid[%+v], log: %s",
			kv.me, kv.gid, kv.config, kv.waitShardIds, message)
	}
}

func (kv *ShardKV) isRepeated(shardId int, clientId int64, opId int64) bool {
	if val, ok := kv.lastMsgIdx[shardId][clientId]; ok {
		return val >= opId
	}
	return false
}

func (kv *ShardKV) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(kv.data) != nil ||
		e.Encode(kv.lastMsgIdx) != nil ||
		e.Encode(kv.config) != nil ||
		e.Encode(kv.oldConfig) != nil ||
		e.Encode(kv.ownShards) != nil ||
		e.Encode(kv.waitShardIds) != nil ||
		e.Encode(kv.historyShards) != nil {
		panic("gen snapshot data encode err")
	}

	return w.Bytes()
}

func (kv *ShardKV) readSnapShotData(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData [shardmaster.NShards]map[string]string
	var lastMsgIdx [shardmaster.NShards]map[int64]int64
	var config shardmaster.Config
	var oldConfig shardmaster.Config
	var ownShards map[int]bool
	var waitShardIds map[int]bool
	var historyShards map[int]map[int]MergeShardData

	if d.Decode(kvData) != nil ||
		d.Decode(lastMsgIdx) != nil ||
		d.Decode(config) != nil ||
		d.Decode(oldConfig) != nil ||
		d.Decode(ownShards) != nil ||
		d.Decode(waitShardIds) != nil ||
		d.Decode(historyShards) != nil {
		log.Fatal("kv read persist err")
	} else {
		kv.data = kvData
		kv.lastMsgIdx = lastMsgIdx
		kv.config = config
		kv.oldConfig = oldConfig
		kv.ownShards = ownShards
		kv.waitShardIds = waitShardIds
		kv.historyShards = historyShards
	}
}

func (kv *ShardKV) configIsReady(configNum int, key string) Err {
	if configNum == 0 || configNum != kv.config.Num {
		kv.log("configReadyErr1")
		return ErrWrongGroup
	}
	shardId := key2shard(key)
	if _, ok := kv.ownShards[shardId]; !ok {
		kv.log("configReadyErr2")
		return ErrWrongGroup
	}
	if _, ok := kv.waitShardIds[shardId]; ok {
		kv.log("configReadyErr3")
		return ErrWrongGroup
	}
	return OK
}

func (kv *ShardKV) pullConfig() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullConfigTimer.C:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				kv.pullConfigTimer.Reset(PullConfigInterval)
				break
			}
			kv.lock("pullConfig")
			lastNum := kv.config.Num
			kv.log(fmt.Sprintf("get last ConfigNum: %v", lastNum))
			kv.unlock("pullConfig")

			config := kv.mck.Query(lastNum + 1)
			if config.Num == lastNum+1 {
				// 找到新的config
				kv.lock("pullConfig")
				// double check
				if len(kv.waitShardIds) == 0 && config.Num == kv.config.Num+1 {
					kv.log(fmt.Sprintf("start config: +%v, lastNum: %d", config.Copy(), lastNum))
					kv.unlock("pullConfig")
					kv.rf.Start(config.Copy())
				} else {
					kv.unlock("pullConfig")
				}
			}
			kv.pullConfigTimer.Reset(PullConfigInterval)
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.stopCh)
	kv.log("kill kv")
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persister = persister

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, kv.gid)
	kv.stopCh = make(chan struct{})
	kv.DebugLog = true

	kv.data = [shardmaster.NShards]map[string]string{}
	for i := range kv.data {
		kv.data[i] = make(map[string]string)
	}

	kv.lastMsgIdx = [shardmaster.NShards]map[int64]int64{}
	for i := range kv.lastMsgIdx {
		kv.lastMsgIdx[i] = make(map[int64]int64)
	}

	kv.waitShardIds = make(map[int]bool)
	kv.historyShards = make(map[int]map[int]MergeShardData)
	config := shardmaster.Config{
		Num:    0,
		Shards: [shardmaster.NShards]int{},
		Groups: map[int][]string{},
	}
	kv.config = config
	kv.oldConfig = config
	kv.readSnapShotData(kv.persister.ReadSnapshot())

	kv.notifyCh = make(map[int64]chan NotifyMsg)
	kv.pullConfigTimer = time.NewTimer(PullConfigInterval)
	kv.pullShardsTimer = time.NewTimer(PullShardsInterval)

	go kv.waitApplyCh()
	go kv.pullConfig()
	go kv.pullShards()

	return kv
}
