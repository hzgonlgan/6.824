package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6_824/labgob"
	"6_824/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Role raft 实例的角色状态
type Role int

const (
	Leader Role = iota
	Follower
	Candidate
)

func (r *Role) String() string {
	if *r == Leader {
		return "Leader"
	}
	if *r == Follower {
		return "Follower"
	}
	if *r == Candidate {
		return "Candidate"
	}
	return "Unknown"
}

const (
	None                  = -1
	tickIntervalMs        = 10
	heartbeatTimeoutTicks = 8
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (rf *Raft) randElectionTimeoutTicks() int {
	return 35 + rand.Intn(15)
}

// LogEntry 日志条目
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role    Role
	votes   int
	timeCh  chan struct{}
	quitCh  chan struct{}
	applyCh chan ApplyMsg
	applyCond *sync.Cond
	// 在所有服务器上的稳定状态
	currentTerm       int
	votedFor          int
	log               []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int
	// 在所有服务器上的非稳定状态
	commitIndex int
	lastApplied int
	// 在领导者上的非稳定状态
	nextIndex  []int
	matchIndex []int
	// 记录剩下的滴答数
	leftHeartbeatTicks int
	leftElectionTicks  int
}

func (rf *Raft) String() string {
	return fmt.Sprintf("[%d %v term:%d voteFor: %d, logLen: %d, lastIndex: %d, lastTerm: %d, commitIndex: %d, lastApplied: %d]\n",
		rf.me, rf.role.String(), rf.currentTerm, rf.votedFor, len(rf.log)-1, rf.getLastLogIndex(), rf.getLastLogTerm(), rf.commitIndex, rf.lastApplied)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isLeader bool
	term = rf.currentTerm
	isLeader = rf.role == Leader
	return term, isLeader
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getRealIndex(logIndex int) int {
	return logIndex - rf.lastIncludedIndex
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	rf.persister.SaveRaftState(rf.getPersistState())
}

func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("%v fail to read persist", rf.String())
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	isLeader := rf.role == Leader
	if isLeader {
		entry := LogEntry{
			Index:   index,
			Command: command,
			Term:    term,
		}
		rf.log = append(rf.log, entry)
		rf.matchIndex[rf.me] = index
		rf.persist()
		rf.leftHeartbeatTicks = 0
		DPrintf("%v start agreement on entry index: %v", rf.String(), entry.Index)
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.quitCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower
	rf.timeCh = make(chan struct{}, 10)
	rf.quitCh = make(chan struct{})
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.currentTerm = 0
	rf.votedFor = None
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0})
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.leftElectionTicks = rf.randElectionTimeoutTicks()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.lastIncludedIndex

	go rf.tickLoop()
	go rf.applyLoop()
	go rf.mainLoop()
	return rf
}
