package raft

import "time"

func (rf *Raft) mainLoop() {
	for {
		select {
		case <-rf.timeCh:
			rf.timeHandler()
		case <-rf.quitCh:
			return
		}
	}
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

func (rf *Raft) applyLoop() {
	rf.mu.Lock()
	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {
			// 释放锁期间，可能由于安装 Leader 发送的快照而改变 lastApplied，故应先增加
			rf.lastApplied++
			entry := rf.log[rf.getRealIndex(rf.lastApplied)]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			rf.mu.Unlock()
			select {
			case rf.applyCh <- applyMsg:
			case <-rf.quitCh:
				return
			}
			rf.mu.Lock()
			DPrintf("%v applied entry index: %v", rf.String(), entry.Index)
		} else {
			rf.applyCond.Wait()
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) timeHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == Leader {
		rf.leftHeartbeatTicks--
		if rf.leftHeartbeatTicks <= 0 {
			rf.leftHeartbeatTicks = heartbeatTimeoutTicks
			rf.broadcastHeartbeat()
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
		panic("unknown raft role")
	}
}

func (rf *Raft) beCandidate() {
	rf.role = Candidate
	DPrintf("%v change to candidate", rf.String())
}

func (rf *Raft) beFollower(term int) {
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = None
	if rf.role == Leader {
		rf.leftElectionTicks = rf.randElectionTimeoutTicks()
	}
	DPrintf("%v change to follower", rf.String())
}

func (rf *Raft) beLeader() {
	rf.role = Leader
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.matchIndex[i] = rf.getLastLogIndex()
		} else {
			rf.matchIndex[i] = 0
		}
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.leftHeartbeatTicks = heartbeatTimeoutTicks
	rf.broadcastHeartbeat()
	DPrintf("%v change to leader", rf.String())
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.broadcastRequestVote()
}
