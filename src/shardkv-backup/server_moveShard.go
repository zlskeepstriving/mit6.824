package shardkv

import (
	"fmt"
	"time"

	"../shardmaster"
)

func (kv *ShardKV) FetchShardData(args *FetchShardDataArgs, reply *FetchShardDataReply) {
	kv.lock("FetchShardData")
	defer kv.unlock("FetchShardData")
	defer kv.log(fmt.Sprintf("resp FetchShardData, args: %+v, reply: %+v", *args, *reply))

	if args.ConfigNum >= kv.config.Num {
		reply.Success = false
		return
	}

	if configData, ok := kv.historyShards[args.ConfigNum]; ok {
		if shardData, ok := configData[args.ShardNum]; ok {
			reply.Success = true
			reply.Data = make(map[string]string)
			reply.MsgIndexes = make(map[int64]int64)
			for k, v := range shardData.MsgIndexes {
				reply.MsgIndexes[k] = v
			}
			for k, v := range shardData.Data {
				reply.Data[k] = v
			}
		}
	}
}

func (kv *ShardKV) CleanShardData(args *CleanShardDataArgs, reply *CleanShardDataReply) {
	kv.lock("CleanShardData")
	kv.log("[Enter CleanShardData]")
	defer kv.log(fmt.Sprintf("resp CleanShardData, args: %+v, reply: %+v", *args, *reply))

	if args.ConfigNum >= kv.config.Num {
		reply.Success = false
		kv.unlock("CleanShardData")
		return
	}
	kv.unlock("CleanShardData")

	_, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		return
	}
	// 简单处理下。。。
	for i := 0; i < shardmaster.NShards; i++ {
		kv.lock("cleanShardData")
		exist := kv.historyDataExist(args.ConfigNum, args.ShardNum)
		kv.unlock("cleanShardData")
		if !exist {
			reply.Success = true
			return
		}
		time.Sleep(time.Millisecond * 20)
	}
	return
}

func (kv *ShardKV) reqCleanUpShardData(shardId int, config shardmaster.Config) {
	configNum := config.Num
	args := CleanShardDataArgs{
		ConfigNum: config.Num,
		ShardNum:  shardId,
	}

	t := time.NewTimer(ReqCleanShardDataTimeout)
	defer t.Stop()

	for {
		for _, server := range config.Groups[config.Shards[shardId]] {
			reply := CleanShardDataReply{}
			to := kv.make_end(server)
			done := make(chan bool, 1)
			r := false

			go func(args *CleanShardDataArgs, reply *CleanShardDataReply) {
				done <- to.Call("ShardKV.CleanShardData", args, reply)
			}(&args, &reply)

			t.Reset(ReqCleanShardDataTimeout)

			select {
			case <-kv.stopCh:
				return
			case r = <-done:
			case <-t.C:
			}
			if r && reply.Success {
				return
			}
		}
		kv.lock("reqCleanUpShardData")
		if kv.config.Num != configNum+1 || len(kv.waitShardIds) == 0 {
			kv.unlock("reqCleanUpShardData")
			break
		}
		kv.unlock("reqCleanUpShardData")
	}
}

func (kv *ShardKV) pullShards() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullShardsTimer.C:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				kv.pullShardsTimer.Reset(PullShardsInterval)
				continue
			}
			kv.lock("pullShards")
			for shardId, need := range kv.waitShardIds {
				if need {
					go kv.pullShard(kv.oldConfig, shardId)
				}
			}
			kv.unlock("pullShards")
			kv.pullConfigTimer.Reset(PullShardsInterval)
		}
	}
}

func (kv *ShardKV) pullShard(config shardmaster.Config, shardId int) {
	lastNum := config.Num
	args := FetchShardDataArgs{
		ConfigNum: config.Num,
		ShardNum:  shardId,
	}
	for _, server := range config.Groups[config.Shards[shardId]] {
		reply := FetchShardDataReply{}
		to := kv.make_end(server)

		if ok := to.Call("ShardKV.FetchShardData", args, reply); ok {
			if reply.Success {
				kv.lock("pullShard")
				if _, ok := kv.waitShardIds[shardId]; ok && kv.config.Num == lastNum+1 {
					replyCopy := reply.Copy()
					mergeArgs := MergeShardData{
						ConfigNum:  args.ConfigNum,
						ShardNum:   args.ShardNum,
						Data:       replyCopy.Data,
						MsgIndexes: replyCopy.MsgIndexes,
					}
					kv.log(fmt.Sprintf("pullShard get data: +%v", mergeArgs))
					kv.unlock("pullShard")
					_, _, isLeader := kv.rf.Start(mergeArgs)
					if !isLeader {
						break
					}
				} else {
					kv.unlock("pullShard")
				}
			}
		}
	}
}
