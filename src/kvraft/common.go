package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id     int64
	SeqNum int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id     int64
	SeqNum int64
}

type GetReply struct {
	Err   Err
	Value string
}

func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("[Id: %v, SeqNum: %v, Op: %v]", args.Id, args.SeqNum, args.Op)
}

func (args *PutAppendReply) String() string {
	return fmt.Sprintf("[Err: %v]", args.Err)
}

func (args *GetArgs) String() string {
	return fmt.Sprintf("[Id: %v, SeqNum: %v]\n", args.Id, args.SeqNum)
}

func (args *GetReply) String() string {
	return fmt.Sprintf("[Err: %v]", args.Err)
}
