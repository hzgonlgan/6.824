package shardkv

import (
	"time"
)

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:   Get,
		Key:    args.Key,
		Id:     args.Id,
		SeqNum: args.SeqNum,
	}
	reply.Err, reply.Value = kv.handleRequest(&op)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:   args.Op,
		Key:    args.Key,
		Value:  args.Value,
		Id:     args.Id,
		SeqNum: args.SeqNum,
	}
	reply.Err, _ = kv.handleRequest(&op)
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	reply.Ok = false
	if args.Num > kv.config.Num {
		kv.mu.Unlock()
		return
	}
	if args.Num < kv.config.Num || !kv.pendingShards[args.Id] {
		reply.Ok = true
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{
		Type: InShard,
		Args: *args,
	}
	err, _ := kv.handleRequest(&op)
	if err == OK {
		reply.Ok = true
	}
}

func (kv *ShardKV) handleRequest(op *Op) (err Err, value string) {
	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	notifyCh := kv.getNotifyCh(index, true)
	kv.DPrintf("%v process request: %v", kv.String(), *op)
	kv.mu.Unlock()
	select {
	case result := <-notifyCh:
		if !isMatch(op, &result) {
			err = ErrWrongLeader
		} else {
			err = result.Err
			value = result.Value
		}
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
		return
	case <-time.After(OpTimeout):
		err = ErrWrongLeader
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
		return
	case <-kv.quitCh:
		return
	}
}
