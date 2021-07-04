package kvraft

import (
	"6_824/labgob"
	"6_824/labrpc"
	"6_824/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Debug     = false
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

func (srcOp *Op) equal(desOp *Op) bool {
	return srcOp.Id == desOp.Id && srcOp.SeqNum == desOp.SeqNum
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// snapshot state
	database    map[string]string
	lastSeqNums map[int64]int64
	lastApplied int

	persister *raft.Persister
	notifyChs map[int]chan Op
	quitCh    chan struct{}
}

func (kv *KVServer) String() string {
	return fmt.Sprintf("[Id: %v, lastApplied: %v]\n", kv.me, kv.lastApplied)
}

func (kv *KVServer) getNotifyCh(index int) chan Op {
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan Op, 1)
	}
	return kv.notifyChs[index]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:    args.Key,
		Type:   "Get",
		Id:     args.Id,
		SeqNum: args.SeqNum,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	DPrintf("%v process Get: %v", kv.String(), args.String())
	notifyCh := kv.getNotifyCh(index)
	kv.mu.Unlock()
	select {
	case result := <-notifyCh:
		kv.mu.Lock()
		if !op.equal(&result) {
			reply.Err = ErrWrongLeader
			delete(kv.notifyChs, index)
			kv.mu.Unlock()
			return
		}
		if value, ok := kv.database[op.Key]; ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
		return
	case <-time.After(OpTimeout):
		kv.mu.Lock()
		reply.Err = ErrWrongLeader
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
		return
	case <-kv.quitCh:
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:    args.Key,
		Value:  args.Value,
		Type:   args.Op,
		Id:     args.Id,
		SeqNum: args.SeqNum,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	DPrintf("%v process PutAppend: %v", kv.String(), args.String())
	notifyCh := kv.getNotifyCh(index)
	kv.mu.Unlock()
	select {
	case result := <-notifyCh:
		kv.mu.Lock()
		if !op.equal(&result) {
			reply.Err = ErrWrongLeader
			delete(kv.notifyChs, index)
			kv.mu.Unlock()
			return
		}
		reply.Err = OK
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
		return
	case <-time.After(OpTimeout):
		kv.mu.Lock()
		reply.Err = ErrWrongLeader
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
		return
	case <-kv.quitCh:
		return
	}
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex != kv.lastApplied+1 {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
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
				kv.getNotifyCh(msg.CommandIndex) <- op
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

func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	return kv.persister.RaftStateSize() >= kv.maxraftstate
}

func (kv *KVServer) takeSnapshot(index int) {
	DPrintf("%v start take snapshot", kv.String())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.database)
	e.Encode(kv.lastSeqNums)
	e.Encode(kv.lastApplied)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) applySnapshot(snapshot []byte) {
	DPrintf("%v start apply snapshot", kv.String())
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var database map[string]string
	var lastSeqNums map[int64]int64
	var lastApplied int
	if d.Decode(&database) != nil ||
		d.Decode(&lastSeqNums) != nil ||
		d.Decode(&lastApplied) != nil {
		DPrintf("%v fail to apply snapshot", kv.String())
	} else {
		kv.database = database
		kv.lastSeqNums = lastSeqNums
		kv.lastApplied = lastApplied
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
	close(kv.quitCh)
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
	kv.database = make(map[string]string)
	kv.lastSeqNums = make(map[int64]int64)
	kv.lastApplied = 0
	kv.persister = persister
	kv.notifyChs = make(map[int]chan Op)
	kv.quitCh = make(chan struct{})
	kv.applySnapshot(persister.ReadSnapshot())

	go kv.applier()

	return kv
}
