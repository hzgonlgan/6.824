package shardctrler

import (
	"6_824/labgob"
	"6_824/raft"
	"log"
	"sync/atomic"
	"time"
)
import "6_824/labrpc"
import "sync"

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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	quitCh  chan struct{}
	dead    int32
	// Your data here.
	configs     []Config // indexed by config num
	lastSeqNums map[int64]int64
	notifyChs   map[int]chan Op
}

func (sc *ShardCtrler) getNotifyCh(index int, createIfNotExist bool) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, ok := sc.notifyChs[index]; !ok {
		if !createIfNotExist {
			return nil
		}
		sc.notifyChs[index] = make(chan Op, 1)
	}
	return sc.notifyChs[index]
}

func (sc *ShardCtrler) deleteNotifyCh(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.notifyChs, index)
}

type Type int

const (
	Join = iota
	Leave
	Move
	Query
)

func (srcOp *Op) equal(desOp *Op) bool {
	return srcOp.Id == desOp.Id && srcOp.SeqNum == desOp.SeqNum
}

type Op struct {
	// Your data here.
	Type   Type
	Args   interface{}
	Id     int64
	SeqNum int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		Type:   Join,
		Args:   *args,
		Id:     args.Id,
		SeqNum: args.SeqNum,
	}
	reply.WrongLeader = sc.handleRequest(&op)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Type:   Leave,
		Args:   *args,
		Id:     args.Id,
		SeqNum: args.SeqNum,
	}
	reply.WrongLeader = sc.handleRequest(&op)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Type:   Move,
		Args:   *args,
		Id:     args.Id,
		SeqNum: args.SeqNum,
	}
	reply.WrongLeader = sc.handleRequest(&op)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		Type:   Query,
		Args:   *args,
		Id:     args.Id,
		SeqNum: args.SeqNum,
	}
	reply.WrongLeader = sc.handleRequest(&op)
	if !reply.WrongLeader {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		args := op.Args.(QueryArgs)
		reply.Config = *sc.processQuery(&args)
	}
}

func (sc *ShardCtrler) handleRequest(op *Op) (wrongLeader bool) {
	wrongLeader = true
	index, _, isLeader := sc.rf.Start(*op)
	if !isLeader {
		return
	}
	notifyCh := sc.getNotifyCh(index, true)
	select {
	case result := <-notifyCh:
		sc.deleteNotifyCh(index)
		if op.equal(&result) {
			wrongLeader = false
		}
		return
	case <-time.After(OpTimeout):
		sc.deleteNotifyCh(index)
		return
	case <-sc.quitCh:
		return
	}
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			if !msg.CommandValid {
				continue
			}
			op := msg.Command.(Op)
			sc.mu.Lock()
			sc.applyOp(&op)
			sc.mu.Unlock()
			if notifyCh := sc.getNotifyCh(msg.CommandIndex, false); notifyCh != nil {
				notifyCh <- op
			}
		case <-sc.quitCh:
			return
		}
	}
}

func (sc *ShardCtrler) applyOp(op *Op) {
	if lastSeqNums, ok := sc.lastSeqNums[op.Id]; ok && op.SeqNum <= lastSeqNums {
		return
	}
	sc.lastSeqNums[op.Id] = op.SeqNum
	if op.Type == Query {
		return
	}
	var newConfig *Config
	switch op.Type {
	case Join:
		args := op.Args.(JoinArgs)
		newConfig = sc.processJoin(&args)
	case Leave:
		args := op.Args.(LeaveArgs)
		newConfig = sc.processLeave(&args)
	case Move:
		args := op.Args.(MoveArgs)
		newConfig = sc.processMove(&args)
	default:
		panic("unknown op type")
	}
	sc.configs = append(sc.configs, *newConfig)
}

func (sc *ShardCtrler) processJoin(args *JoinArgs) *Config {
	newConfig := sc.createNewConfig()
	for gid, servers := range args.Servers {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newConfig.Groups[gid] = newServers
	}
	return sc.rebalance(newConfig)
}

func (sc *ShardCtrler) processLeave(args *LeaveArgs) *Config {
	newConfig := sc.createNewConfig()
	for _, gid := range args.GIDs {
		delete(newConfig.Groups, gid)
	}
	isLeave := make(map[int]bool)
	for _, v := range args.GIDs {
		isLeave[v] = true
	}
	for idx, gid := range newConfig.Shards {
		if isLeave[gid] {
			newConfig.Shards[idx] = 0
		}
	}
	return sc.rebalance(newConfig)
}

func (sc *ShardCtrler) processMove(args *MoveArgs) *Config {
	newConfig := sc.createNewConfig()
	newConfig.Shards[args.Shard] = args.GID
	return newConfig
}

func (sc *ShardCtrler) processQuery(args *QueryArgs) *Config {
	if args.Num >= 0 && args.Num < len(sc.configs) {
		return &sc.configs[args.Num]
	} else {
		return &sc.configs[len(sc.configs)-1]
	}
}

func (sc *ShardCtrler) createNewConfig() *Config {
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Shards: oldConfig.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}
	return &newConfig
}

func (sc *ShardCtrler) rebalance(config *Config) *Config {
	groupCnt := len(config.Groups)
	if groupCnt == 0 {
		return config
	}
	cntMap := make(map[int]int)
	for gid := range config.Groups {
		cntMap[gid] = 0
	}
	for _, gid := range config.Shards {
		if gid != 0 {
			cntMap[gid]++
		}
	}

	average := NShards / groupCnt
	remain := NShards % groupCnt
	bestAssign := make([]int, groupCnt)
	for i := 0; i < groupCnt; i++ {
		if remain > 0 {
			bestAssign[i] = average + 1
		} else {
			bestAssign[i] = average
		}
		remain--
	}

	gidArr := make([]int, 0, groupCnt)
	for gid, _ := range cntMap {
		gidArr = append(gidArr, gid)
	}
	for i := 0; i < groupCnt-1; i++ {
		for j := groupCnt - 1; j > i; j-- {
			if cntMap[gidArr[j]] > cntMap[gidArr[j-1]] ||
				(cntMap[gidArr[j]] == cntMap[gidArr[j-1]] &&
					gidArr[j] > gidArr[j-1]) {
				temp := gidArr[j]
				gidArr[j] = gidArr[j-1]
				gidArr[j-1] = temp
			}
		}
	}

	for i := 0; i < groupCnt; i++ {
		if cntMap[gidArr[i]] > bestAssign[i] {
			freeGid := gidArr[i]
			freeCnt := cntMap[gidArr[i]] - bestAssign[i]
			for idx, gid := range config.Shards {
				if gid == freeGid {
					config.Shards[idx] = 0
					freeCnt--
					if freeCnt <= 0 {
						break
					}
				}
			}
		}
	}
	for i := 0; i < groupCnt; i++ {
		if cntMap[gidArr[i]] < bestAssign[i] {
			assignGid := gidArr[i]
			assignCnt := bestAssign[i] - cntMap[gidArr[i]]
			for idx, gid := range config.Shards {
				if gid == 0 {
					config.Shards[idx] = assignGid
					assignCnt--
					if assignCnt <= 0 {
						break
					}
				}
			}
		}
	}

	return config
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
	close(sc.quitCh)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc := new(ShardCtrler)
	sc.me = me
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.quitCh = make(chan struct{})
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	// Your code here.
	sc.lastSeqNums = make(map[int64]int64)
	sc.notifyChs = make(map[int]chan Op)

	go sc.applier()

	return sc
}
