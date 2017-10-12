package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type LogItem struct {
	Key   string
	Value string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//raft提交的日志
	logs   map[int]LogItem   // copy of raft's committed entries
	maplog map[string]string //用map形式存储的kv形式日志，方便查询和更改
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.WrongLeader = false

	//我不是leader
	if kv.rf.IsLeader() == false {
		reply.WrongLeader = true
		reply.Err = "I'm not Leader"
		return
	}

	v, ok := kv.maplog[args.Key]
	//key不存在或者还没有提交
	if ok == false {
		reply.Err = "Key is not exist."
		return
	}

	reply.Value = v
	reply.Err = ""

	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.WrongLeader = false

	value := LogItem{args.Key, args.Value}
	index, term, isLeader := kv.rf.Start(value)

	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = "I'm not Leader"
		return
	}

	if index < 0 || term < 0 {
		reply.Err = "Add log to raft fail."
		return
	}

	reply.Err = ""

	return
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 1， the k/v server should store snapshots with persister.SaveSnapshot(),
// 2， and Raft should save its state (including log) with persister.SaveRaftState().
// 3， the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// 4， in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// 	   you don't need to snapshot.
// 5， StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func(me int) {
		for m := range kv.applyCh {
			err_msg := ""
			if m.UseSnapshot {
				// ignore the snapshot
			} else if v, ok := (m.Command).(LogItem); ok {
				kv.mu.Lock()
				_, prevok := kv.logs[m.Index-1]
				kv.logs[m.Index] = v
				kv.maplog[v.Key] = v.Value
				kv.mu.Unlock()

				if m.Index > 1 && prevok == false {
					err_msg = fmt.Sprintf("server %v apply out of order %v", me, m.Index)
				}
			} else {
				err_msg = fmt.Sprintf("committed command %v is not an LogItem", m.Command)
			}

			if err_msg != "" {
				fmt.Printf("server:%v configlog:%v \n", me, kv.logs)
				log.Fatalf("apply error: %v\n", err_msg)
				// keep reading after error so that Raft doesn't block
				// holding locks...
			}
		}
	}(me)

	return kv
}
