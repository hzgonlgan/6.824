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
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	//	"bytes"
	"sync"
	//	"6_824/labgob"
	"6_824/labrpc"
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
	heartbeatTimeoutTicks = 10
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (rf *Raft) randElectionTimeoutTicks() int {
	return 30 + rand.Intn(20)
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
	// 在所有服务器上的稳定状态
	currentTerm int
	votedFor    int
	log         []LogEntry
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

func (rf *Raft) raftInfo() string {
	return fmt.Sprintf("[%d %v term:%d voteFor: %d, logLen: %d, commitIndex: %d, lastApplied: %d]\n",
		rf.me, rf.role.String(), rf.currentTerm, rf.votedFor, len(rf.log) - 1, rf.commitIndex, rf.lastApplied)
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
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
}

func (rf *Raft) isMoreUpToDate(destIndex int, destTerm int) bool {
	srcIndex := rf.getLastLogIndex()
	srcTerm := rf.getLastLogTerm()
	return destTerm > srcTerm || (destTerm == srcTerm && destIndex >= srcIndex)
}

func (rf *Raft) tickLoop() {
	for {
		time.Sleep(time.Millisecond * tickIntervalMs)
		select {
		case rf.timeCh <- struct{}{}:
		case <-rf.quitCh:
			return
		}
	}
}

func (rf *Raft) eventLoop() {
	for {
		select {
		case <-rf.timeCh:
			rf.timeHandler()
		case <-rf.quitCh:
			return
		}
	}
}

func (rf *Raft) applyLoop() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 20)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			entry := rf.log[rf.lastApplied+1]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			select {
			case rf.applyCh <- applyMsg:
			case <-rf.quitCh:
				return
			}
			rf.lastApplied++
			DPrintf("%v applied entry index: %v", rf.raftInfo(), entry.Index)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) timeHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == Leader {
		rf.leftHeartbeatTicks--
		if rf.leftHeartbeatTicks <= 0 {
			rf.leftHeartbeatTicks = heartbeatTimeoutTicks
			rf.broadcastAppendEntries()
		}
	} else if rf.role == Follower || rf.role == Candidate {
		rf.leftElectionTicks--
		if rf.leftElectionTicks <= 0 {
			rf.leftElectionTicks = rf.randElectionTimeoutTicks()
			if rf.role == Follower {
				rf.beCandidate()
			}
			rf.startElection()
		}
	} else {
		DPrintf("%v in time_handler, unknown raft role - %v", rf.raftInfo(), rf.role)
	}
}

func (rf *Raft) beCandidate() {
	rf.role = Candidate
	DPrintf("%v change to candidate", rf.raftInfo())
}

func (rf *Raft) beFollower(term int) {
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = None
	if rf.role == Leader {
		rf.leftElectionTicks = rf.randElectionTimeoutTicks()
	}
	DPrintf("%v change to follower", rf.raftInfo())
}

func (rf *Raft) beLeader() {
	rf.role = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.leftHeartbeatTicks = heartbeatTimeoutTicks
	rf.broadcastAppendEntries()
	DPrintf("%v change to leader", rf.raftInfo())
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.broadcastRequestVote()
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v receive vote request %#v", rf.raftInfo(), args)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	// 发现新 Term
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
		rf.persist()
	}

	// 实例为 Follower 才能进入 if，如果实例为 Leader 或者 Candidate 一定为自己投了票
	if args.Term == rf.currentTerm {
		if (rf.votedFor == None || rf.votedFor == args.CandidateId) &&
			rf.isMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.leftElectionTicks = rf.randElectionTimeoutTicks()
			reply.VoteGranted = true
			DPrintf("%v grant vote to %d", rf.raftInfo(), args.CandidateId)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	DPrintf("%v start election", rf.raftInfo())
	rf.votes = 1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		DPrintf("%v send RequestVote to %d", rf.raftInfo(), i)

		go func(id int) {
			reply := &RequestVoteReply{}
			ch := make(chan bool, 1)
			select {
			case ch <- rf.sendRequestVote(id, args, reply):
				ok := <- ch
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.beFollower(reply.Term)
						rf.persist()
						return
					}

					if rf.role != Candidate {
						return
					}

					if reply.VoteGranted {
						rf.votes++
						DPrintf("%v votes: %d, receive vote from %d", rf.raftInfo(), rf.votes, id)
						if rf.votes > len(rf.peers)/2 {
							rf.beLeader()
						}
					}
				}
			case <-rf.quitCh:
			}
		}(i)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, LeaderCommit: %d, EntryLen: %d",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v receive append entries %#v", rf.raftInfo(), args.String())

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
		rf.persist()
	}

	// Term 相同，实例肯定不是 Leader (一个 Term 只能有一个 Leader)
	// 如果为 Candidate，肯定更旧，转为 Follower
	if args.Term == rf.currentTerm {
		if rf.role == Candidate {
			// 发现现任 Leader
			rf.beFollower(args.Term)
			rf.votedFor = args.LeaderId
			rf.persist()
		}
		rf.leftElectionTicks = rf.randElectionTimeoutTicks()
		rf.synchronizeLog(args, reply)
		return
	}
}

func (rf *Raft) synchronizeLog(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.PrevLogIndex > rf.getLastLogIndex() || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		if args.PrevLogIndex <= rf.getLastLogIndex() {
			// 日志冲突
			rf.log = rf.log[0:args.PrevLogIndex]
			rf.persist()
		}
		reply.Success = false
	} else {
		rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = minInt(args.LeaderCommit, rf.getLastLogIndex())
		}
		rf.persist()
		reply.Success = true
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		prevLogIndex := rf.nextIndex[i] - 1
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.log[prevLogIndex].Term,
			Entries:      rf.log[rf.nextIndex[i]:],
			LeaderCommit: rf.commitIndex,
		}
		go func(id int) {
			reply := &AppendEntriesReply{}
			ch := make(chan bool, 1)
			select {
			case ch <- rf.sendAppendEntries(id, args, reply):
				ok := <- ch
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.beFollower(reply.Term)
						rf.persist()
					}

					if rf.role != Leader {
						return
					}

					if reply.Success {
						rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[id] = rf.matchIndex[id] + 1

						quorumIndex := rf.getQuorumIndex()
						// 领导人只能提交当前任期的日志
						if rf.log[quorumIndex].Term == rf.currentTerm && quorumIndex > rf.commitIndex {
							rf.commitIndex = quorumIndex
						}
					} else {
						if args.PrevLogIndex > 0 {
							rf.nextIndex[id]--
						}
					}
				}
			case <-rf.quitCh:
			}
		}(i)
	}
}

func (rf *Raft) getQuorumIndex() int {
	total := len(rf.matchIndex)
	matchIndex := make([]int, total)
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)
	return matchIndex[(total-1)/2]
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
		DPrintf("%v start agreement on entry index: %v", rf.raftInfo(), entry.Index)
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
	rf.currentTerm = 0
	rf.votedFor = None
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.leftElectionTicks = rf.randElectionTimeoutTicks()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.tickLoop()
	go rf.applyLoop()
	go rf.eventLoop()
	return rf
}
