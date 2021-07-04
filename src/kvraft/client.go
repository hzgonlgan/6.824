package kvraft

import (
	"6_824/labrpc"
	"fmt"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me       int64
	seqNum   int64
	leaderId int
}

func (ck *Clerk) String() string {
	return fmt.Sprintf("[Id: %v, SeqNum: %v, LeaderId: %v]", ck.me, ck.seqNum, ck.leaderId)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	ck.seqNum = 0
	ck.leaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &GetArgs{
		Key:    key,
		Id:     ck.me,
		SeqNum: ck.seqNum,
	}
	ck.seqNum++
	for {
		reply := &GetReply{}
		if !ck.servers[ck.leaderId].Call("KVServer.Get", args, reply) ||
			reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			DPrintf("%v args: %v, wrong leader", ck.String(), args.String())
			continue
		}
		if reply.Err == OK {
			DPrintf("%v args: %v, success return: %v", ck.String(), args.String(), reply.Value)
			return reply.Value
		}
		DPrintf("%v args: %v, no key", ck.String(), args.String())
		return ""
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     op,
		Id:     ck.me,
		SeqNum: ck.seqNum,
	}
	ck.seqNum++
	for {
		reply := &PutAppendReply{}
		if !ck.servers[ck.leaderId].Call("KVServer.PutAppend", args, reply) ||
			reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			DPrintf("%v args: %v, wrong leader", ck.String(), args.String())
			continue
		}
		if reply.Err == OK {
			DPrintf("%v args: %v, success return", ck.String(), args.String())
			return
		}
		panic("unknown PutAppend reply")
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
