package kvraft

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrDuplicateRequest = "ErrDuplicateRequest"
	ErrOtherError       = "ErrOtherError"
	ErrTimeout
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	OpName   string // "Put" or "Append"
	ClientId int64
	Seq      int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	OpName   string
	ClientId int64
	Seq      int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
