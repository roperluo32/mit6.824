package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq uint64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	Seq         uint64
	Server      int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq uint64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
	Seq         uint64
	Server      int
}
