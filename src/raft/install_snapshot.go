package raft

import "fmt"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("[Term: %d, LeaderId: %d, LastIncludedIndex: %d, LastIncludedTerm: %d]",
		args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("[Term: %d]", reply.Term)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v receive install snapshot %v", rf.raftInfo(), args.String())

	reply.Term = rf.currentTerm

	needPersist := false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
		needPersist = true
	}

	if rf.role == Candidate {
		rf.beFollower(args.Term)
		rf.votedFor = args.LeaderId
		needPersist = true
	}

	rf.leftElectionTicks = rf.randElectionTimeoutTicks()
	reply.Term = rf.currentTerm

	if needPersist {
		rf.persist()
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) syncInstallSnapshot(i int) {
	DPrintf("%v send install snapshot to %d", rf.raftInfo(), i)
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	go func(id int) {
		reply := &InstallSnapshotReply{}
		ch := make(chan bool, 1)
		select {
		case ch <- rf.sendInstallSnapshot(id, args, reply):
			ok := <-ch
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("%v receive install snapshot reply from %d", rf.raftInfo(), id)

				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					rf.persist()
					return
				}

				if rf.role != Leader || reply.Term < rf.currentTerm {
					return
				}
				rf.matchIndex[i] = rf.lastIncludedIndex
				rf.nextIndex[i] = rf.lastIncludedIndex + 1
			}
		case <-rf.quitCh:
		}
	}(i)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.lastApplied {
		return false
	}

	if lastIncludedIndex >= rf.getLastLogIndex() {
		rf.log = make([]LogEntry, 1)
	} else {
		rf.log = rf.log[rf.getRealIndex(lastIncludedIndex):]
	}
	rf.log[0] = LogEntry{Index: 0, Term: 0}
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), snapshot)
	rf.commitIndex = maxInt(rf.commitIndex, lastIncludedIndex)
	rf.lastApplied = lastIncludedIndex
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.lastApplied {
		return
	}

	realIndex := rf.getRealIndex(index)
	rf.log = rf.log[realIndex:]
	rf.log[0] = LogEntry{Index: 0, Term: 0}
	rf.lastIncludedIndex = rf.log[realIndex].Index
	rf.lastIncludedTerm = rf.log[realIndex].Term
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), snapshot)
}
