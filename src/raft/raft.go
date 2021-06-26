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
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

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
	eventCh chan *raftEvent
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
	return fmt.Sprintf("[%d %v term:%d]", rf.me, rf.role.String(), rf.currentTerm)
}

type raftEvent struct {
	args   interface{}   // 传递参数
	result interface{}   // 返回结果
	ch     chan struct{} // 用于同步
}

func newRaftEvent(args interface{}) *raftEvent {
	return &raftEvent{
		args:   args,
		result: nil,
		ch:     make(chan struct{}, 1),
	}
}

func (ev *raftEvent) wait() {
	<-ev.ch
}

func (ev *raftEvent) done(result interface{}) {
	ev.result = result
	close(ev.ch)
}

func (rf *Raft) tickLoop() {
	for {
		time.Sleep(tickIntervalMs * time.Millisecond)
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
		case ev := <-rf.eventCh:
			rf.eventHandler(ev)
		case <-rf.timeCh:
			rf.timeHandler()
		case <-rf.quitCh:
			return
		}
	}
}

func (rf *Raft) timeHandler() {
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

func (rf *Raft) eventHandler(ev *raftEvent) {
	switch v := ev.args.(type) {
	case *RequestVoteArgs:
		reply := rf.processRequestVoteArgs(v)
		ev.done(reply)
	case *RequestVoteReply:
		rf.processRequestVoteReply(v)
	case *AppendEntriesArgs:
		reply := rf.processAppendEntriesArgs(v)
		ev.done(reply)
	case *AppendEntriesReply:
		rf.processAppendEntriesReply(v)
	case *GetState:
		state := rf.processGetState()
		ev.done(state)
	default:
		DPrintf("%v in event_handler, unknown raft event - %v", rf.raftInfo(), ev)
		ev.done(nil)
	}
}

func (rf *Raft) processRequestVoteArgs(args *RequestVoteArgs) (reply *RequestVoteReply) {
	reply = &RequestVoteReply{}

	DPrintf("%v receive vote request from %d - %v", rf.raftInfo(), args.CandidateId, args)

	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		DPrintf("%v grant vote to %d", rf.raftInfo(), args.CandidateId)
		return
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm {
		if (rf.votedFor == None || rf.votedFor == args.CandidateId) &&
			rf.isMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			DPrintf("%v grant vote to %d", rf.raftInfo(), args.CandidateId)
		} else {
			reply.VoteGranted = false
		}
		reply.Term = rf.currentTerm
		return
	}

	return nil
}

func (rf *Raft) processRequestVoteReply(reply *RequestVoteReply) {
	if reply.Term > rf.currentTerm {
		rf.beFollower(reply.Term)
		return
	}

	if rf.role != Candidate {
		return
	}

	if reply.VoteGranted {
		DPrintf("%v receive votes - %d", rf.raftInfo(), rf.votes)
		rf.votes++
		if rf.votes > len(rf.peers)/2 {
			rf.beLeader()
		}
	}
}

func (rf *Raft) processAppendEntriesArgs(args *AppendEntriesArgs) (reply *AppendEntriesReply) {
	reply = &AppendEntriesReply{}

	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.leftElectionTicks = rf.randElectionTimeoutTicks()
		return
	}

	// 2A do nothing

	return nil
}

func (rf *Raft) processAppendEntriesReply(reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.beFollower(reply.Term)
		return
	}

	if rf.role != Leader {
		return
	}

	// 2A do nothing
}

func (rf *Raft) processGetState() (state *CurrentState) {
	state = &CurrentState{
		currentTerm: rf.currentTerm,
		role:        rf.role,
	}
	return
}

func (rf *Raft) beCandidate() {
	rf.role = Candidate
	DPrintf("%v change to candidate", rf.raftInfo())
}

func (rf *Raft) beFollower(term int) {
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = None
	rf.leftElectionTicks = rf.randElectionTimeoutTicks()
	rf.persist()
	DPrintf("%v change to follower", rf.raftInfo())
}

func (rf *Raft) beLeader() {
	rf.role = Leader
	// rf.persist()
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.leftHeartbeatTicks = heartbeatTimeoutTicks
	rf.broadcastAppendEntries()
	DPrintf("%v change to leader", rf.raftInfo())
}

func (rf *Raft) startElection() {
	DPrintf("%v start election", rf.raftInfo())
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.broadcastRequestVote()
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log)
}

func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := rf.getLastLogIndex()
	if lastLogIndex > 0 {
		return rf.log[lastLogIndex-1].Term
	} else {
		return 0
	}
}

func (rf *Raft) isMoreUpToDate(destIndex int, destTerm int) bool {
	srcIndex := rf.getLastLogIndex()
	srcTerm := rf.getLastLogTerm()
	return destTerm > srcTerm || (destTerm == srcTerm && destIndex >= srcIndex)
}

type GetState struct {
}

type CurrentState struct {
	currentTerm int
	role        Role
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	ev := newRaftEvent(&GetState{})
	select {
	case rf.eventCh <- ev:
	case <-rf.quitCh:
		return -1, false
	}
	ev.wait()
	state := ev.result.(*CurrentState)
	return state.currentTerm, state.role == Leader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	ev := newRaftEvent(args)
	select {
	case rf.eventCh <- ev:
	case <-rf.quitCh:
		return
	}
	ev.wait()
	*reply = *(ev.result.(*RequestVoteReply))
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) broadcastRequestVote() {
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
		go func(id int) {
			reply := &RequestVoteReply{}
			//DPrintf("%v send RequestVote to %d", rf.raftInfo(), id)
			if rf.sendRequestVote(id, args, reply) {
				select {
				case rf.eventCh <- newRaftEvent(reply):
				case <-rf.quitCh:
				}
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ev := newRaftEvent(args)
	select {
	case rf.eventCh <- ev:
	case <-rf.quitCh:
		return
	}
	ev.wait()
	*reply = *(ev.result.(*AppendEntriesReply))
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
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		go func(id int) {
			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(id, args, reply) {
				select {
				case rf.eventCh <- newRaftEvent(reply):
				case <-rf.quitCh:
				}
			}
		}(i)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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

func init() {
	rand.Seed(time.Now().Unix())
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
	rf.eventCh = make(chan *raftEvent, 10)
	rf.timeCh = make(chan struct{}, 10)
	rf.quitCh = make(chan struct{})
	rf.currentTerm = 0
	rf.votedFor = None
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.leftElectionTicks = rf.randElectionTimeoutTicks()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("%v election timeout %d", rf.raftInfo(), rf.leftElectionTicks)

	go rf.tickLoop()
	go rf.eventLoop()

	return rf
}
