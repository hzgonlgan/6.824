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
	DPrintf("%v receive append entries %v", rf.raftInfo(), args.String())

	reply.Term = rf.currentTerm
	reply.Success = false

	needPersist := false

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
	rf.synchronizeLog(args, reply)

	if needPersist {
		rf.persist()
	}

	DPrintf("%v reply append entries %v", rf.raftInfo(), reply.String())
}

func (rf *Raft) synchronizeLog(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.PrevLogIndex > rf.getLastLogIndex() || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		if args.PrevLogIndex <= rf.getLastLogIndex() {
			idx := args.PrevLogIndex
			for rf.log[idx-1].Term == rf.log[args.PrevLogIndex].Term {
				idx--
			}
			reply.ConflictIndex = idx
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

			// 解决日志冲突
			rf.log = rf.log[0:args.PrevLogIndex]
			rf.persist()
		} else {
			reply.ConflictIndex = rf.getLastLogIndex() + 1
			reply.ConflictTerm = -1
		}
		reply.Success = false
	} else {
		// 如果 rpc 乱序，旧的 rpc 请求可能使之前提交的日志被截断，这是错误的
		if args.PrevLogIndex+len(args.Entries) > rf.getLastLogIndex() {
			rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...)
		} else {
			for i := 0; i < len(args.Entries); i++ {
				rf.log[args.PrevLogIndex+1+i] = args.Entries[i]
			}
		}
		rf.persist()
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = minInt(args.LeaderCommit, rf.getLastLogIndex())
		}
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
		entries := rf.log[rf.nextIndex[i]:]
		entriesCopy := make([]LogEntry, len(entries))
		copy(entriesCopy, entries)
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.log[prevLogIndex].Term,
			Entries:      entriesCopy,
			LeaderCommit: rf.commitIndex,
		}
		DPrintf("%v send append entries to %d", rf.raftInfo(), i)
		go func(id int) {
			reply := &AppendEntriesReply{}
			ch := make(chan bool, 1)
			select {
			case ch <- rf.sendAppendEntries(id, args, reply):
				ok := <-ch
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					DPrintf("%v receive append entries reply from %d", rf.raftInfo(), id)

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
						if rf.log[quorumIndex].Term == rf.currentTerm && quorumIndex > rf.commitIndex {
							rf.commitIndex = quorumIndex
						}
					} else {
						rf.nextIndex[id] = reply.ConflictIndex
						if reply.ConflictTerm != -1 && reply.ConflictTerm < args.PrevLogTerm {
							idx := args.PrevLogIndex
							for rf.log[idx].Term > reply.ConflictTerm {
								idx--
							}
							if rf.log[idx].Term == reply.ConflictTerm {
								rf.nextIndex[id] = idx + 1
							}
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
