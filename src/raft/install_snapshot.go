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
	needPersist := false
	defer func() {
		reply.Term = rf.currentTerm
		if needPersist {
			rf.persist()
		}
	}()
	DPrintf("%v receive install snapshot %v", rf.String(), args.String())

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

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	rf.leftElectionTicks = rf.randElectionTimeoutTicks()

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

	DPrintf("%v reply install snapshot %v", rf.String(), reply.String())
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) syncInstallSnapshot(i int) {
	DPrintf("%v send install snapshot to %d", rf.String(), i)
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
				DPrintf("%v receive install snapshot reply from %d", rf.String(), id)

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

	if lastIncludedIndex <= rf.commitIndex  {
		return false
	}

	if lastIncludedIndex >= rf.getLastLogIndex() {
		rf.log = make([]LogEntry, 1)
	} else {
		rf.log = rf.log[rf.getRealIndex(lastIncludedIndex):]
	}
	rf.log[0] = LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm}
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), snapshot)
	rf.commitIndex = lastIncludedIndex
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
	DPrintf("%v creat snapshot before index: %d", rf.String(), index)

	if index <= rf.lastIncludedIndex {
		return
	}

	realIndex := rf.getRealIndex(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[realIndex].Term
	rf.log = rf.log[realIndex:]
	rf.log[0] = LogEntry{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm}
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), snapshot)
}
