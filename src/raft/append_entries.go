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

	ConflictTerm  int
	ConflictIndex int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("[Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, LeaderCommit: %d, EntryLen: %d]",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("[Term: %d, Success: %v, ConflictTerm: %d, ConflictIndex: %d]",
		reply.Term, reply.Success, reply.ConflictTerm, reply.ConflictIndex)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	needPersist := false
	defer func() {
		reply.Term = rf.currentTerm
		if needPersist {
			rf.persist()
		}
	}()
	DPrintf("%v receive append entries %v", rf.String(), args.String())

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
		needPersist = true
	}

	// Term 相同，实例肯定不是 Leader (一个 Term 只能有一个 Leader)
	// 如果为 Candidate，肯定更旧，发现现任 Leader，转为 Follower
	if rf.role == Candidate {
		rf.beFollower(args.Term)
		rf.votedFor = args.LeaderId
		needPersist = true
	}

	rf.leftElectionTicks = rf.randElectionTimeoutTicks()
	rf.synchronizeLog(args, reply, &needPersist)

	DPrintf("%v reply append entries %v", rf.String(), reply.String())
}

func (rf *Raft) synchronizeLog(args *AppendEntriesArgs, reply *AppendEntriesReply, needPersist *bool) {
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = rf.lastIncludedIndex+1
		reply.ConflictTerm = -1
		reply.Success = false
		return
	}

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = -1
		reply.Success = false
		return
	}
	// lastIncludeIndex <= prevLogIndex <= lastLogIndex
	// 0 <= getRealIndex(prevLogIndex) <= len(log) - 1
	if args.PrevLogTerm != rf.log[rf.getRealIndex(args.PrevLogIndex)].Term {
		idx := args.PrevLogIndex
		for rf.getRealIndex(idx-1) > 0 &&
			rf.log[rf.getRealIndex(idx-1)].Term ==
			rf.log[rf.getRealIndex(args.PrevLogIndex)].Term {
			idx--
		}
		reply.ConflictIndex = idx
		reply.ConflictTerm = rf.log[rf.getRealIndex(args.PrevLogIndex)].Term
		reply.Success = false
		// 解决日志冲突
		rf.log = rf.log[0:rf.getRealIndex(args.PrevLogIndex)]
		*needPersist = true
		return
	}

	// 如果 rpc 乱序，旧的 rpc 请求可能使之前提交的日志被截断，这是错误的
	for idx, entry := range args.Entries {
		if entry.Index > rf.getLastLogIndex() ||
			entry.Term != rf.log[rf.getRealIndex(entry.Index)].Term {
			rf.log  = append(rf.log[0:rf.getRealIndex(entry.Index)], args.Entries[idx:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, rf.getLastLogIndex())
		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
	}
	reply.Success = true
	*needPersist = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			rf.syncInstallSnapshot(i)
		} else {
			rf.syncAppendEntries(i)
		}

	}
}

func (rf *Raft) syncAppendEntries(i int) {
	DPrintf("%v send append entries to %d", rf.String(), i)
	prevLogIndex := rf.nextIndex[i] - 1
	entries := rf.log[rf.getRealIndex(rf.nextIndex[i]):]
	entriesCopy := make([]LogEntry, len(entries))
	copy(entriesCopy, entries)
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[rf.getRealIndex(prevLogIndex)].Term,
		Entries:      entriesCopy,
		LeaderCommit: rf.commitIndex,
	}
	go func(id int) {
		reply := &AppendEntriesReply{}
		ch := make(chan bool, 1)
		select {
		case ch <- rf.sendAppendEntries(id, args, reply):
			ok := <-ch
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("%v receive append entries reply from %d", rf.String(), id)

				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					rf.persist()
					return
				}

				if rf.role != Leader || reply.Term < rf.currentTerm {
					return
				}

				if reply.Success {
					rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[id] = rf.matchIndex[id] + 1
					quorumIndex := rf.getQuorumIndex()
					// 领导人只能提交当前任期的日志
					if quorumIndex > rf.commitIndex && rf.log[rf.getRealIndex(quorumIndex)].Term == rf.currentTerm {
						rf.commitIndex = quorumIndex
						if rf.commitIndex > rf.lastApplied {
							rf.applyCond.Signal()
						}
					}
				} else {
					rf.nextIndex[id] = reply.ConflictIndex
					if reply.ConflictTerm != -1 && reply.ConflictTerm < args.PrevLogTerm {
						idx := args.PrevLogIndex
						for rf.getRealIndex(idx) > 0 && rf.log[rf.getRealIndex(idx)].Term > reply.ConflictTerm {
							idx--
						}
						if rf.getRealIndex(idx) > 0 {
							rf.nextIndex[id] = idx + 1
						}
					}
				}
			}
		case <-rf.quitCh:
		}
	}(i)
}

func (rf *Raft) getQuorumIndex() int {
	total := len(rf.matchIndex)
	matchIndex := make([]int, total)
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)
	return matchIndex[(total-1)/2]
}
