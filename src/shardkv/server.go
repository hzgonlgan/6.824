package shardkv

import (
	"6_824/labrpc"
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
	Debug     = true
	OpTimeout = time.Millisecond * 500
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key    string
	Value  string
	Type   string // "Get" or "Put" or "Append"
	Id     int64
	SeqNum int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	quitCh       chan struct{}
	dead         int32
	// Your definitions here.
	database    map[string]string
	lastSeqNums map[int64]int64

	persister *raft.Persister
	notifyChs map[int]chan Op
}

func (srcOp *Op) equal(desOp *Op) bool {
	return srcOp.Id == desOp.Id && srcOp.SeqNum == desOp.SeqNum
}

func (kv *ShardKV) String() string {
	return fmt.Sprintf("[Id: %v, gid: %v]\t", kv.me, kv.gid)
}

func (kv *ShardKV) getNotifyCh(index int, createIfNotExist bool) chan Op {
	if _, ok := kv.notifyChs[index]; !ok {
		if !createIfNotExist {
			return nil
		}
		kv.notifyChs[index] = make(chan Op, 1)
	}
	return kv.notifyChs[index]
}

func (kv *ShardKV) deleteNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.notifyChs, index)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:    args.Key,
		Type:   "Get",
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
	case ret := <-notifyCh:
		kv.mu.Lock()
		if !op.equal(&ret) {
			err = ErrWrongLeader
		} else {
			err = OK
			if op.Type == "Get" {
				if v, ok := kv.database[op.Key]; ok {
					value = v
				} else {
					err = ErrNoKey
				}
			}
		}
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
		return
	case <-time.After(OpTimeout):
		err = ErrWrongLeader
		kv.deleteNotifyCh(index)
		return
	case <-kv.quitCh:
		return
	}
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)
				if lastSeqNum, ok := kv.lastSeqNums[op.Id]; !ok || op.SeqNum > lastSeqNum {
					switch op.Type {
					case "Put":
						kv.database[op.Key] = op.Value
					case "Append":
						kv.database[op.Key] += op.Value
					}
					kv.lastSeqNums[op.Id] = op.SeqNum
				}
				kv.getNotifyCh(msg.CommandIndex, false) <- op
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

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.quitCh = make(chan struct{})

	kv.database = make(map[string]string)
	kv.lastSeqNums = make(map[int64]int64)
	kv.persister = persister
	kv.notifyChs = make(map[int]chan Op)

	kv.applySnapshot(persister.ReadSnapshot())

	go kv.applier()

	return kv
}
