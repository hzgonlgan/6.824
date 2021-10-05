package shardkv

import (
	"6_824/labrpc"
	"6_824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6_824/raft"
import "sync"
import "6_824/labgob"

const (
	Debug                = true
	OpTimeout            = time.Millisecond * 500
	ConfigUpdateInterval = time.Millisecond * 100
)

func init() {
	log.SetFlags(0)
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	Get      = "Get"
	Put      = "Put"
	Append   = "Append"
	Config   = "Config"
	InShard  = "InShard"
	OutShard = "OutShard"
)

type Op struct {
	Type   string
	Key    string
	Value  string
	Id     int64
	SeqNum int64
	Config *shardctrler.Config
	Args   *MigrateArgs
}

type Result struct {
	Type     string
	Err      Err
	Value    string
	Id       int64
	SeqNum   int64
	ShardId  int
	ShardNum int
}

func (op *Op) isMatch(result *Result) bool {
	if op.Type != result.Type {
		return false
	}
	switch op.Type {
	case Get:
		fallthrough
	case Put:
		fallthrough
	case Append:
		return op.Id == result.Id && op.SeqNum == result.SeqNum
	case InShard:
		fallthrough
	case OutShard:
		args := op.Args
		return args.Id == result.ShardId && args.Num == result.ShardNum
	default:
		return false
	}
}

func (kv *ShardKV) isDuplicate(op *Op) bool {
	switch op.Type {
	case Put:
		fallthrough
	case Append:
		if lastSeqNums, ok := kv.lastSeqNums[op.Id]; !ok || op.SeqNum > lastSeqNums {
			kv.lastSeqNums[op.Id] = op.SeqNum
			return false
		} else {
			return true
		}
	default:
		return true
	}
}

type Shard struct {
	Database    map[string]string
	LastSeqNums map[int64]int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	mck          *shardctrler.Clerk
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	quitCh       chan struct{}
	dead         int32
	// Your definitions here.
	config        shardctrler.Config
	shards        map[int]Shard
	servedShards  map[int]bool
	pendingShards []int

	persister *raft.Persister
	notifyChs map[int]chan Result

	database    map[string]string
	lastSeqNums map[int64]int64
}

func (kv *ShardKV) String() string {
	return fmt.Sprintf("[Id: %v, gid: %v]\t", kv.me, kv.gid)
}

func (kv *ShardKV) getNotifyCh(index int, createIfNotExist bool) chan Result {
	if _, ok := kv.notifyChs[index]; !ok {
		if !createIfNotExist {
			return nil
		}
		kv.notifyChs[index] = make(chan Result, 1)
	}
	return kv.notifyChs[index]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:    args.Key,
		Type:   Get,
		Id:     args.Id,
		SeqNum: args.SeqNum,
	}
	reply.Err, reply.Value = kv.handleRequest(&op)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:    args.Key,
		Value:  args.Value,
		Type:   args.Op,
		Id:     args.Id,
		SeqNum: args.SeqNum,
	}
	reply.Err, _ = kv.handleRequest(&op)
}

func (kv *ShardKV) handleRequest(op *Op) (err Err, value string) {
	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	notifyCh := kv.getNotifyCh(index, true)
	DPrintf("%v process request: %v", kv.String(), *op)
	kv.mu.Unlock()
	select {
	case result := <-notifyCh:
		if _, isLeader := kv.rf.GetState(); isLeader {
			fmt.Printf("op: %v, result: %v\n", op, result)
		}
		kv.mu.Lock()
		if !op.isMatch(&result) {
			err = ErrWrongLeader
		} else {
			err = result.Err
			value = result.Value
		}
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

func (kv *ShardKV) commandApplier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)
				result := kv.applyOp(&op)
				if notifyCh := kv.getNotifyCh(msg.CommandIndex, false); notifyCh != nil {
					notifyCh <- *result
				}
				if kv.needSnapshot() {
					kv.takeSnapshot(msg.CommandIndex)
				}
				kv.mu.Unlock()
				continue
			}
			if msg.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.applySnapshot(msg.Snapshot)
				}
				kv.mu.Unlock()
				continue
			}
			panic("unknown apply msg")
		case <-kv.quitCh:
			return
		}
	}
}

func (kv *ShardKV) applyOp(op *Op) (result *Result) {
	result = &Result{
		Type:   op.Type,
		Err:    OK,
		Id:     op.Id,
		SeqNum: op.SeqNum,
	}
	switch op.Type {
	case Get:
		kv.processGet(op, result)
	case Put:
		kv.processPut(op, result)
	case Append:
		kv.processAppend(op, result)
	}
	return result
}

func (kv *ShardKV) processGet(op *Op, result *Result) {
	if value, ok := kv.database[op.Key]; ok {
		result.Value = value
	} else {
		result.Err = ErrNoKey
	}
}

func (kv *ShardKV) processPut(op *Op, result *Result) {
	if kv.isDuplicate(op) {
		return
	}
	kv.database[op.Key] = op.Value
}

func (kv *ShardKV) processAppend(op *Op, result *Result) {
	if kv.isDuplicate(op) {
		return
	}
	kv.database[op.Key] += op.Value
}

func (kv *ShardKV) processConfig(op *Op, result *Result) {

}

func (kv *ShardKV) processInShard(op *Op, result *Result) {

}

func (kv *ShardKV) processOutShard(op *Op, result *Result) {

}

func (kv *ShardKV) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	return kv.persister.RaftStateSize() >= kv.maxraftstate
}

func (kv *ShardKV) takeSnapshot(index int) {
	DPrintf("%v start take snapshot", kv.String())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.database)
	e.Encode(kv.lastSeqNums)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	DPrintf("%v start apply snapshot", kv.String())
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var database map[string]string
	var lastSeqNums map[int64]int64
	if d.Decode(&database) != nil ||
		d.Decode(&lastSeqNums) != nil {
		DPrintf("%v fail to apply snapshot", kv.String())
	} else {
		kv.database = database
		kv.lastSeqNums = lastSeqNums
	}
}

func (kv *ShardKV) configUpdater() {
	for !kv.killed() {
		kv.mu.Lock()
		curNum := kv.config.Num
		kv.mu.Unlock()
		if config := kv.mck.Query(curNum + 1); config.Num == curNum+1 {

		}
		time.Sleep(ConfigUpdateInterval)
	}
}

func (kv *ShardKV) shardSender() {
	for !kv.killed() {
		kv.mu.Lock()
		curNum := kv.config.Num
		kv.mu.Unlock()
		newConfig := kv.mck.Query(-1)
		if newConfig.Num > curNum {

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
	// Your code here, if desired.
	close(kv.quitCh)
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
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// makeEnd(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and makeEnd() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = makeEnd
	kv.gid = gid
	kv.ctrlers = ctrlers
	// Your initialization code here.

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.quitCh = make(chan struct{})

	kv.shards = make(map[int]Shard)
	kv.config = kv.mck.Query(-1)

	kv.persister = persister
	kv.notifyChs = make(map[int]chan Result)

	//kv.applySnapshot(persister.ReadSnapshot())

	kv.database = make(map[string]string)
	kv.lastSeqNums = make(map[int64]int64)
	go kv.commandApplier()
	//go kv.configUpdater()
	//go kv.shardSender()

	return kv
}
