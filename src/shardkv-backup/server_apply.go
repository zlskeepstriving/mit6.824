package shardkv

import (
	"fmt"
	"time"

	"../raft"
	"../shardmaster"
)

func (kv *ShardKV) dataGet(key string) (err Err, val string) {
	if v, ok := kv.data[key2shard(key)][key]; ok {
		err = OK
		val = v
		return
	}
	err = ErrNoKey
	return err, ""
}

func (kv *ShardKV) waitApplyCh() {
	defer func() {
		// for debug
		kv.log(fmt.Sprintf("kv killed:%v", kv.killed()))
		time.Sleep(time.Millisecond * 1000)
		kv.log(fmt.Sprintf("kv applych killed, applych len: %d", len(kv.applyCh)))
	}()
	for {
		select {
		case <-kv.stopCh:
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				kv.log(fmt.Sprintf("get install snapshot, idx: %d", msg.CommandIndex))
				kv.lock("readSnapshotData")
				kv.readSnapShotData(kv.persister.ReadSnapshot())
				kv.unlock("readSnapshotData")
				continue
			}
			kv.log(fmt.Sprintf("get applymsg: idx:%d, msg:%+v", msg.CommandIndex, msg))
			if op, ok := msg.Command.(Op); ok {
				kv.applyOp(msg, op)
			} else if config, ok := msg.Command.(shardmaster.Config); ok {
				kv.applyConfig(msg, config)
			} else if mergeData, ok := msg.Command.(MergeShardData); ok {
				kv.applyMergeShardData(msg, mergeData)
			} else if cleanUp, ok := msg.Command.(CleanShardDataArgs); ok {
				kv.applyCleanUp(msg, cleanUp)
			} else {
				panic("unknown apply msg")
			}
		}
	}
}

func (kv *ShardKV) applyOp(msg raft.ApplyMsg, op Op) {
	msgIdx := msg.CommandIndex
	kv.lock("waitApplyCh")
	defer kv.unlock("waitApplyCh")
	kv.log(fmt.Sprintf("in applyOp:%+v", op))

	shardId := key2shard(op.Key)
	isRepeated := kv.isRepeated(shardId, op.ClientId, op.MsgId)
	if kv.configIsReady(op.ConfigNum, op.Key) == OK {
		switch op.Op {
		case "Put":
			if !isRepeated {
				kv.data[shardId][op.Key] = op.Value
				kv.lastMsgIdx[shardId][op.ClientId] = op.MsgId
			}
		case "Append":
			if !isRepeated {
				_, v := kv.dataGet(op.Key)
				kv.data[shardId][op.Key] = v + op.Value
				kv.lastMsgIdx[shardId][op.ClientId] = op.MsgId
			}
		case "Get":
		default:
			panic(fmt.Sprintf("unknown method: %s", op.Op))
		}
		kv.log(fmt.Sprintf("apply op: %v", op))
		kv.saveSnapshot(msgIdx)
		if ch, ok := kv.notifyCh[op.ReqId]; ok {
			notifyMsg := NotifyMsg{Err: OK}
			if op.Op == "Get" {
				notifyMsg.Err, notifyMsg.Value = kv.dataGet(op.Key)
			}
			ch <- notifyMsg
		}
	} else {
		// config not ready
		if ch, ok := kv.notifyCh[op.ReqId]; ok {
			ch <- NotifyMsg{Err: ErrWrongGroup}
		}
		return
	}
}

func (kv *ShardKV) saveSnapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() > kv.maxraftstate*9/10 {
		go kv.rf.SnapShot(index, kv.genSnapshotData())
	}
}

func (kv *ShardKV) applyConfig(msg raft.ApplyMsg, config shardmaster.Config) {
	kv.lock("applyConfig")
	defer kv.unlock("applyConfig")

	if config.Num <= kv.config.Num {
		kv.saveSnapshot(msg.CommandIndex)
		return
	}

	if config.Num != kv.config.Num+1 {
		return
	}

	oldConfig := kv.config.Copy()
	ownShardIds := make([]int, 0, shardmaster.NShards)
	waitShardIds := make([]int, 0, shardmaster.NShards)
	deleteShardIds := make([]int, 0, shardmaster.NShards)
	for i := 0; i < shardmaster.NShards; i++ {
		if config.Shards[i] == kv.gid {
			ownShardIds = append(ownShardIds, i)
			if oldConfig.Shards[i] != kv.gid {
				waitShardIds = append(waitShardIds, i)
			}
		} else {
			if oldConfig.Shards[i] == kv.gid {
				deleteShardIds = append(deleteShardIds, i)
			}
		}
	}
	d := make(map[int]MergeShardData)
	for _, id := range deleteShardIds {
		m := MergeShardData{
			ConfigNum:  oldConfig.Num,
			ShardNum:   id,
			MsgIndexes: make(map[int64]int64),
			Data:       map[string]string{},
		}
		for k, v := range kv.lastMsgIdx[id] {
			m.MsgIndexes[k] = v
		}
		for k, v := range kv.data[id] {
			m.Data[k] = v
		}
		kv.lastMsgIdx[id] = make(map[int64]int64)
		kv.data[id] = make(map[string]string)
		d[id] = m
	}
	kv.historyShards[oldConfig.Num] = d

	kv.ownShards = make(map[int]bool)
	for _, id := range ownShardIds {
		kv.ownShards[id] = true
	}

	kv.waitShardIds = make(map[int]bool)
	for _, id := range waitShardIds {
		kv.waitShardIds[id] = true
	}

	kv.config = config.Copy()
	kv.oldConfig = oldConfig
	kv.saveSnapshot(msg.CommandIndex)
}

func (kv *ShardKV) applyMergeShardData(msg raft.ApplyMsg, data MergeShardData) {
	kv.lock("applyMergeShardData")
	defer kv.unlock("applyMergeShardData")

	if data.ConfigNum != kv.config.Num-1 {
		return
	}
	if exist, _ := kv.waitShardIds[data.ShardNum]; exist {
		kv.lastMsgIdx[data.ShardNum] = make(map[int64]int64)
		for k, v := range data.MsgIndexes {
			kv.lastMsgIdx[data.ShardNum][k] = v
		}
		kv.data[data.ShardNum] = make(map[string]string)
		for k, v := range data.Data {
			kv.data[data.ShardNum][k] = v
		}
	}

	delete(kv.waitShardIds, data.ShardNum)
	go kv.reqCleanUpShardData(data.ShardNum, kv.oldConfig)
}

func (kv *ShardKV) historyDataExist(configNum int, shardNum int) bool {
	if _, ok := kv.historyShards[configNum]; ok {
		if _, ok := kv.historyShards[configNum][shardNum]; ok {
			return true
		}
	}
	return false
}

func (kv *ShardKV) applyCleanUp(msg raft.ApplyMsg, data CleanShardDataArgs) {
	kv.lock("applyCleanUp")
	defer kv.unlock("applyCleanUp")
	kv.log(fmt.Sprintf("applyCleanUp, msg: %v, data: %v", msg, data))
	if kv.historyDataExist(data.ConfigNum, data.ShardNum) {
		delete(kv.historyShards[data.ConfigNum], data.ShardNum)
	}
	kv.saveSnapshot(msg.CommandIndex)
}
