package raft

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

func (rf *Raft) isMoreUpToDate(destIndex int, destTerm int) bool {
	srcIndex := rf.getLastLogIndex()
	srcTerm := rf.getLastLogTerm()
	return destTerm > srcTerm || (destTerm == srcTerm && destIndex >= srcIndex)
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

