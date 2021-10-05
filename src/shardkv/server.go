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
	Debug                = false
	OpTimeout            = time.Millisecond * 500
	ConfigUpdateInterval = time.Millisecond * 100
	ShardMigrateInterval = time.Millisecond * 100
)

func init() {
	log.SetFlags(0)
}

func (kv *ShardKV) DPrintf(format string, a ...interface{}) {
	if Debug {
		//if _, isLeader := kv.rf.GetState(); isLeader && kv.gid == 100 {
			log.Printf(format, a...)
			log.Printf("")
		//}
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
	Config shardctrler.Config
	Args   MigrateArgs
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

func isMatch(op *Op, result *Result) bool {
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
		lastSeqNums := kv.shards[key2shard(op.Key)].LastSeqNums
		if lastSeqNum, ok := lastSeqNums[op.Id]; !ok || op.SeqNum > lastSeqNum {
			lastSeqNums[op.Id] = op.SeqNum
			return false
		} else {
			return true
		}
	default:
		return true
	}
}

func (kv *ShardKV) canServe(op *Op) bool {
	shard := key2shard(op.Key)
	return kv.config.Shards[shard] == kv.gid && !kv.pendingShards[shard]
}

type Shard struct {
	Database    map[string]string
	LastSeqNums map[int64]int64
}

func makeShard() *Shard {
	shard := &Shard{
		Database:    make(map[string]string),
		LastSeqNums: make(map[int64]int64),
	}
	return shard
}

func copyShard(src *Shard) *Shard {
	des := makeShard()
	for k, v := range src.Database {
		des.Database[k] = v
	}
	for k, v := range src.LastSeqNums {
		des.LastSeqNums[k] = v
	}
	return des
}

func (kv *ShardKV) hasShardsToMigrate() bool {
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.pendingShards[i] && kv.config.Shards[i] != kv.gid {
			return true
		}
	}
	return false
}

func (kv *ShardKV) getShardsToMigrate() ([]MigrateArgs, [][]string) {
	argss := make([]MigrateArgs, 0)
	serverss := make([][]string, 0)
	for i, pending := range kv.pendingShards {
		gid := kv.config.Shards[i]
		if pending && gid != kv.gid {
			shard := kv.shards[i]
			args := MigrateArgs{
				Id:    i,
				Num:   kv.config.Num,
				Shard: *copyShard(&shard),
			}
			argss = append(argss, args)
			servers := kv.config.Groups[gid]
			serverss = append(serverss, servers)
		}
	}
	return argss, serverss
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
	migrateCond  *sync.Cond
	persister    *raft.Persister
	notifyChs    map[int]chan Result
	// Your definitions here.
	config        shardctrler.Config
	shards        map[int]Shard
	pendingShards [shardctrler.NShards]bool
}

func (kv *ShardKV) String() string {
	return fmt.Sprintf("[Id: %v, gid: %v, pending: %v]\n[config: %v]\n", kv.me, kv.gid, kv.pendingShards, kv.config.Shards)
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

func (kv *ShardKV) commandApplier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)
				result := kv.applyOp(&op)
				kv.DPrintf("%vop: %v, result: %v", kv.String(), op, result)
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
	case Config:
		kv.processConfig(op, result)
	case InShard:
		kv.processInShard(op, result)
	case OutShard:
		kv.processOutShard(op, result)
	}
	return result
}

func (kv *ShardKV) processGet(op *Op, result *Result) {
	if !kv.canServe(op) {
		result.Err = ErrWrongGroup
		return
	}
	database := kv.shards[key2shard(op.Key)].Database
	if value, ok := database[op.Key]; ok {
		result.Value = value
	} else {
		result.Err = ErrNoKey
	}
}

func (kv *ShardKV) processPut(op *Op, result *Result) {
	if !kv.canServe(op) {
		result.Err = ErrWrongGroup
		return
	}
	if kv.isDuplicate(op) {
		return
	}
	database := kv.shards[key2shard(op.Key)].Database
	database[op.Key] = op.Value
}

func (kv *ShardKV) processAppend(op *Op, result *Result) {
	if !kv.canServe(op) {
		result.Err = ErrWrongGroup
		return
	}
	if kv.isDuplicate(op) {
		return
	}
	database := kv.shards[key2shard(op.Key)].Database
	database[op.Key] += op.Value
}

func (kv *ShardKV) processConfig(op *Op, result *Result) {
	newConfig := op.Config
	if newConfig.Num != kv.config.Num+1 {
		return
	}
	for _, pending := range kv.pendingShards {
		if pending {
			return
		}
	}
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.config.Shards[i] == 0 {
			if newConfig.Shards[i] == kv.gid {
				kv.shards[i] = *makeShard()
			}
			continue
		}
		if (kv.config.Shards[i] == kv.gid) == (newConfig.Shards[i] == kv.gid) {
			continue
		}
		if newConfig.Shards[i] == 0 {
			delete(kv.shards, i)
			continue
		}
		kv.pendingShards[i] = true
	}
	kv.config = newConfig
	kv.migrateCond.Signal()
}

func (kv *ShardKV) processInShard(op *Op, result *Result) {
	args := op.Args
	if args.Num > kv.config.Num {
		result.Err = ErrWrongGroup
		return
	}
	if args.Num < kv.config.Num {
		result.Err = OK
		return
	}
	result.Err = OK
	if !kv.pendingShards[args.Id] || kv.config.Shards[args.Id] != kv.gid {
		return
	}
	kv.pendingShards[args.Id] = false
	kv.shards[args.Id] = *copyShard(&args.Shard)
}

func (kv *ShardKV) processOutShard(op *Op, result *Result) {
	args := op.Args
	if args.Num != kv.config.Num {
		return
	}
	if !kv.pendingShards[args.Id] {
		return
	}
	kv.pendingShards[args.Id] = false
	delete(kv.shards, args.Id)
}

func (kv *ShardKV) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	return kv.persister.RaftStateSize() >= kv.maxraftstate
}

func (kv *ShardKV) takeSnapshot(index int) {
	kv.DPrintf("%v start take snapshot", kv.String())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.config)
	e.Encode(kv.shards)
	e.Encode(kv.pendingShards)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	kv.DPrintf("%v start apply snapshot", kv.String())
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var config shardctrler.Config
	var shards map[int]Shard
	var pendingShards [shardctrler.NShards]bool
	if d.Decode(&config) != nil ||
		d.Decode(&shards) != nil ||
		d.Decode(&pendingShards) != nil {
		kv.DPrintf("%v fail to apply snapshot", kv.String())
	} else {
		kv.config = config
		kv.shards = shards
		kv.pendingShards = pendingShards
	}
}

func (kv *ShardKV) configUpdater() {
	for !kv.killed() {
		kv.mu.Lock()
		curNum := kv.config.Num
		kv.mu.Unlock()
		if config := kv.mck.Query(curNum + 1); config.Num == curNum+1 {
			op := Op{
				Type:   Config,
				Config: config,
			}
			kv.rf.Start(op)
		}
		time.Sleep(ConfigUpdateInterval)
	}
}

func (kv *ShardKV) shardMigrator() {
	kv.mu.Lock()
	for !kv.killed() {
		if kv.hasShardsToMigrate() {
			argss, serverss := kv.getShardsToMigrate()
			kv.mu.Unlock()
			var wg sync.WaitGroup
			for i, args := range argss {
				wg.Add(1)
				go func(args MigrateArgs, servers []string) {
					kv.sendShards(&args, servers)
					wg.Done()
				}(args, serverss[i])
			}
			wg.Wait()
			time.Sleep(ShardMigrateInterval)
			kv.mu.Lock()
		} else {
			kv.migrateCond.Wait()
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) sendShards(args *MigrateArgs, servers []string) {
	for _, server := range servers {
		end := kv.makeEnd(server)
		var reply MigrateReply
		newArgs := MigrateArgs{
			Id:  args.Id,
			Num: args.Num,
		}
		ok := end.Call("ShardKV.Migrate", args, &reply)
		if ok && reply.Ok {
			kv.mu.Lock()
			if newArgs.Num != kv.config.Num || !kv.pendingShards[newArgs.Id] {
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			op := Op{
				Type: OutShard,
				Args: newArgs,
			}
			kv.rf.Start(op)
			return
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
	kv.migrateCond = sync.NewCond(&kv.mu)
	kv.persister = persister
	kv.notifyChs = make(map[int]chan Result)
	kv.shards = make(map[int]Shard)

	kv.applySnapshot(persister.ReadSnapshot())

	go kv.commandApplier()
	go kv.configUpdater()
	go kv.shardMigrator()

	return kv
}
