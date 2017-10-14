package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

var gIdCount uint = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastleader int //最近可用的leader索引

	cid    uint   //客户端编号
	reqseq uint64 //发包的唯一序列号
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
	ck.lastleader = -1

	ck.cid = gIdCount
	gIdCount++

	ck.reqseq = 0

	return ck
}

func (ck *Clerk) generateSeq() uint64 {
	prex := uint64(ck.cid * uint(0xffffffff))

	seq := prex + ck.reqseq

	ck.reqseq++

	return seq
}

func (ck *Clerk) GetInst(args *GetArgs, server int) (bool, string) {
	reply := &GetReply{}
	ok := ck.servers[server].Call("RaftKV.Get", args, reply)

	//网络超时
	if ok == false {
		DPrintf("Client [Get] server:%v return false. Maybe network not well....  args:%v", server, args)
		return false, ""
	}

	DPrintf("Client [Get] Reply from server:%v reply:%v. args:%v, seq:%v", server, reply, args, args.Seq)

	//Get返回成功
	if reply.WrongLeader == false && reply.Err == "" {
		DPrintf("Client [Get] server:%v reply success. args:%v, seq:%v", server, args, args.Seq)
		return true, reply.Value
	}

	return false, ""
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	args := &GetArgs{Key: key, Seq: ck.generateSeq()}

	// You will have to modify this function.

	DPrintf("[Client] [Get] ===============Start to send Get req. key:%v, seq:%v", args.Key, args.Seq)

	//使用上次成功访问过的leader
	if ck.lastleader >= 0 {
		DPrintf("[Client] [Get] use lastleader:%v", ck.lastleader)
		success, val := ck.GetInst(args, ck.lastleader)

		//Get返回成功
		if success {
			return val
		}

		//访问lastleader失败，将其置为-1
		ck.lastleader = -1
		DPrintf("[Client] [Get] use lastleader:%v fail", ck.lastleader)
	}

	for {
		for i := range ck.servers {
			success, val := ck.GetInst(args, i)

			//Get返回成功
			if success {
				ck.lastleader = i
				return val
			}
		}
	}

	return ""
}

func (ck *Clerk) PutAppendInst(args *PutAppendArgs, server int) bool {
	reply := &PutAppendReply{}
	ok := ck.servers[server].Call("RaftKV.PutAppend", args, reply)

	//网络超时
	if ok == false {
		DPrintf("[Client] [PutAppendInst] server:%v return false. Maybe network not well....  args:%v", server, args)
		return false
	}

	DPrintf("[Client] [PutAppendInst] Reply from server:%v reply:%v. args:%v, seq:%v", server, reply, args, args.Seq)

	//Get返回成功
	if reply.WrongLeader == false && reply.Err == "" {
		DPrintf("[Client] [PutAppendInst] server:%v reply success. args:%v", server, args)
		return true
	}

	return false
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := &PutAppendArgs{Key: key, Value: value, Op: op, Seq: ck.generateSeq()}

	DPrintf("[Client] [PutAppend] =========Start PutAppend req :%v, key:%v, value:%v, op:%v", args, key, value, op)
	//使用上次成功访问过的leader
	if ck.lastleader > 0 {
		DPrintf("[Client] [PutAppend] use lastleader:%v", ck.lastleader)

		success := ck.PutAppendInst(args, ck.lastleader)

		//Get返回成功
		if success {
			return
		}

		//访问lastleader失败，将其置为-1
		ck.lastleader = -1
		DPrintf("[Client] [PutAppend] use lastleader:%v failed", ck.lastleader)
	}

	for {
		for i := range ck.servers {
			DPrintf("[Client] [PutAppend] Send PutAppend to server :%v", i)
			success := ck.PutAppendInst(args, i)

			//Get返回成功
			if success {
				ck.lastleader = i
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
