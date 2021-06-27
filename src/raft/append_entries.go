package raft

import (
	"fmt"
	"sort"
)

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

