package raft

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

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

}

func (rf *Raft) sendInstallSnapshot(server int) {
	var args *InstallSnapshotArgs
	var reply *InstallSnapshotReply
	ch := make(chan bool, 1)
	ch <- rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	ok := <-ch
	ok = !ok
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
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//
	//if index < rf.commitIndex {
	//	return
	//}
	//
	//state := rf.persister.ReadRaftState()
	//rf.persister.SaveStateAndSnapshot(state, snapshot)
	//rf.log = rf.log[index + 1:]
}
