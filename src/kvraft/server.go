package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		format = format + "\n"
		fmt.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string

	Seq    uint64
	Server int
}

type ReqLogInfo struct {
	Server int
	Index  int
	Term   int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//raft提交的日志
	logs   map[int]Op        // copy of raft's committed entries
	maplog map[string]string //用map形式存储的kv形式日志，方便查询和更改
	mapch  map[int]chan int  //当位于index的日志提交时，就往对应的channel放入对应日志的term，以通知相关的协程，比如append

	mapseqcommited map[uint64]ReqLogInfo //保存提交日志的seq，用来去重
	mapseqreqqed   map[uint64]ReqLogInfo //保存addAppend请求的日志seq，用来去重

	debugSwitch bool
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.WrongLeader = false
	reply.Seq = args.Seq
	reply.Server = kv.me

	kv.DPrintf("[Get], Receive get req:%v, key:%v, seq:%v", args, args.Key, args.Seq)

	//我不是leader
	if kv.rf.IsLeader() == false {
		reply.WrongLeader = true
		reply.Err = "I'm not Leader"
		kv.DPrintf("[Get], NOT Leader. args:%v", args)
		return
	}

	kv.DPrintf("[Get] theleader start to process req:%v", args.Seq)

	value := Op{Key: args.Key, Op: "Get", Seq: args.Seq, Server: kv.me}

	//检查具有[seq]请求是否需要再次提交
	dup := kv.checkDupReq(value)
	if dup == 1 {
		//按照提交成功的返回值来返回
		reply.Err = "" //"Err: Seq has duplicated.."
		reply.WrongLeader = false

		v, ok := kv.maplog[args.Key]
		//key不存在或者还没有提交
		if ok == false {
			reply.Err = "Key is not exist."
			kv.DPrintf("[Get], Key is not exist. args:%v", args)
			return
		}

		reply.Value = v
		return
	}

	index, term, _ := kv.rf.Start(value)

	if index < 0 || term < 0 {
		reply.Err = "Add log to raft fail."
		kv.DPrintf("[Get] Add log to raft fail. index:%v, term:%v, args:%v", index, term, args)
		return
	}

	kv.mu.Lock()
	//记录请求
	kv.recodeReq(args.Seq, index)

	//监听index对应的channel，以确定log是否提交
	_, ok := kv.mapch[index]
	if ok == false {
		kv.mapch[index] = make(chan int, 1)
	}
	notifych := kv.mapch[index]

	kv.mu.Unlock()

	//轮询检查
	for {
		time.Sleep(20 * time.Millisecond)

		//日志已经提交
		if len(notifych) > 0 {
			_ = <-notifych

			reply.Err = ""
			reply.WrongLeader = false

			kv.DPrintf("[Get] req commit ok. index:%v, term:%v. reply:%v", index, term, reply)
			break
		}

		//leader关系检查
		t, leader := kv.rf.GetState()
		if t != term || leader == false {
			reply.Err = "I'm not Leader when commit log"
			kv.DPrintf("[Get], I'm not Leader when commit log. index:%v, term:%v", index, term)
			reply.WrongLeader = true
			return
		}
	}

	v, ok := kv.maplog[args.Key]
	//key不存在或者还没有提交
	if ok == false {
		reply.Err = "Key is not exist."
		kv.DPrintf("[Get], Key is not exist. args:%v", args)
		return
	}

	reply.Value = v
	reply.Err = ""

	kv.DPrintf("[Get], Get Success. args:%v, reply:%v", args, reply)
	return
}

func checkOpEqual(value1 interface{}, value2 interface{}) bool {
	v1, ok := value1.(Op)
	if ok == false {
		return false
	}

	v2, ok := value2.(Op)
	if ok == false {
		return false
	}

	if v1.Seq == v2.Seq {
		return true
	}
	return false
}

//1,返回true，说明是重复日志，丢弃
//2， 返回false，说明没有提交过或者提交过但是已经失效了，可以继续提交
//返回值说明：
//	0:	没有提交过的请求
//	1:	已经提交的重复请求
//	2:	在raft但是尚未提交的请求
func (kv *RaftKV) checkDupReq(op Op) int {
	seq := op.Seq
	//1，是否已经提交过了（检查终点）
	_, ok := kv.mapseqcommited[seq]
	if ok {
		kv.DPrintf("[checkDupReq] seq:%v has commited...", seq)
		return 1
	}

	//2,是否有请求过（检查起点）
	loginfo, ok := kv.mapseqreqqed[seq]
	if ok == false {
		dup := kv.rf.CheckInNotCommitLog(op, checkOpEqual)
		return 2 //没有请求过
	} else {
		//有请求过，检查其分配的索引是否已经提交了日志
		log, ok := kv.logs[loginfo.Index]
		if ok {
			//提交了别的seq的日志
			if log.Seq != seq {
				return 0
			}
		}

		//分配的索引超过了raft目前的日志索引，说明是一个已经失效的日志
		if loginfo.Index > kv.rf.CurrentIndex() {
			return 0 //失效的日志，可以继续提交
		}
	}

	kv.DPrintf("[checkDupReq] seq:%v is duplicated", seq)
	return 1
}

//记录PutAppend请求
func (kv *RaftKV) recodeReq(seq uint64, index int) {
	loginfo, ok := kv.mapseqreqqed[seq]
	if ok == false {
		loginfo = ReqLogInfo{}
	}

	loginfo.Index = index
	loginfo.Server = kv.me

	kv.mapseqreqqed[seq] = loginfo

	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	reply.WrongLeader = false
	reply.Seq = args.Seq
	reply.Server = kv.me

	if kv.rf.IsLeader() == false {
		reply.WrongLeader = true
		prefix := "I'm not Leader. server: "
		reply.Err = Err(prefix + strconv.Itoa(kv.me))
		kv.DPrintf("[PutAppend], Not Leader. args:%v", args)
		return
	}

	value := Op{args.Key, args.Value, args.Op, args.Seq, kv.me}

	//检查具有[seq]请求是否需要再次提交
	dup := kv.checkDupReq(value)
	if dup != 0 {
		//按照提交成功的返回值来返回
		reply.Err = "" //"Err: Seq has duplicated.."
		reply.WrongLeader = false
		return
	}

	kv.DPrintf("[PutAppend] Receive req... args:%v, key:%s, value:%s, Op:%v, seq:%v", args, args.Key, args.Value, value, args.Seq)

	index, term, _ := kv.rf.Start(value)

	kv.DPrintf("[PutAppend] theleader start to process req:%v", args.Seq)

	if index < 0 || term < 0 {
		reply.Err = "Add log to raft fail."
		kv.DPrintf("[PutAppend] Add log to raft fail. index:%v, term:%v, args:%v", index, term, args)
		return
	}

	kv.mu.Lock()
	//记录请求
	kv.recodeReq(args.Seq, index)

	//监听index对应的channel，以确定log是否提交
	_, ok := kv.mapch[index]
	if ok == false {
		kv.mapch[index] = make(chan int, 1)
	}
	notifych := kv.mapch[index]

	kv.mu.Unlock()

	//轮询检查
	for {
		time.Sleep(20 * time.Millisecond)

		//日志已经提交
		if len(notifych) > 0 {
			_ = <-notifych

			reply.Err = ""
			reply.WrongLeader = false

			kv.DPrintf("[PutAppend] req commit ok. index:%v, term:%v. reply:%v", index, term, reply)
			return
		}

		//leader关系检查
		t, leader := kv.rf.GetState()
		if t != term || leader == false {
			reply.Err = "I'm not Leader when commit log"
			kv.DPrintf("[PutAppend], I'm not Leader when commit log. index:%v, term:%v", index, term)
			reply.WrongLeader = true
			return
		}
	}

	return
}

func (kv *RaftKV) DPrintf(format string, a ...interface{}) (n int, err error) {
	if kv.debugSwitch == false {
		return
	}

	format = "[kv:%v] " + format
	b := make([]interface{}, 0)
	b = append(b, kv.me)
	for i := range a {
		b = append(b, a[i])
	}

	DPrintf(format, b...)
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

	kv.debugSwitch = false
}

func (kv *RaftKV) applyLog(m raft.ApplyMsg, v Op) string {

	err_msg := ""

	//检查日志index是否合法
	_, prevok := kv.logs[m.Index-1]
	if m.Index > 1 && prevok == false {
		err_msg = fmt.Sprintf("server %v apply out of order %v", kv.me, m.Index)
	}

	//将提交的日志的值保存到数据库中，目前用map来保存
	kv.logs[m.Index] = v

	kv.DPrintf("[applyLog], commited logs:%v", kv.logs)

	switch v.Op {
	case "Put":
		kv.maplog[v.Key] = v.Value
	case "Append":
		_, ok := kv.maplog[v.Key]
		if ok {
			//key对应的值存在，那么追加到后面
			kv.maplog[v.Key] = kv.maplog[v.Key] + v.Value
		} else {
			//key对应的值不存在，那么就直接赋值
			kv.maplog[v.Key] = v.Value
		}
	default:
		break
	}

	//向监听index对应的channel中放入1，以通知Get日志已经提交
	_, ok := kv.mapch[m.Index]
	if ok == false {
		kv.mapch[m.Index] = make(chan int, 1)
	}
	kv.mapch[m.Index] <- 1

	//检查提交的日志是否已经提交过
	_, ok = kv.mapseqcommited[v.Seq]
	if ok == true {
		//已经提交过
		err_msg = fmt.Sprintf("server %v apply seq:%v has commited.prev commit server:%v index:%v, now commit server:%v index:%v, OP:%v",
			kv.me, v.Seq, kv.mapseqcommited[v.Seq].Server, kv.mapseqcommited[v.Seq].Index, v.Server, m.Index, v)
	}
	loginfo := ReqLogInfo{Server: v.Server, Index: m.Index}
	kv.mapseqcommited[v.Seq] = loginfo

	return err_msg
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

	kv.debugSwitch = true
	kv.logs = make(map[int]Op)
	kv.maplog = make(map[string]string)
	kv.mapch = make(map[int]chan int)

	kv.mapseqcommited = make(map[uint64]ReqLogInfo)
	kv.mapseqreqqed = make(map[uint64]ReqLogInfo)

	// You may need initialization code here.
	go func(me int) {
		for m := range kv.applyCh {
			err_msg := ""
			if m.UseSnapshot {
				// ignore the snapshot
			} else if v, ok := (m.Command).(Op); ok {
				kv.mu.Lock()

				//将raft提交的日志应用到数据库中
				err_msg = kv.applyLog(m, v)

				kv.mu.Unlock()

			} else {
				err_msg = fmt.Sprintf("committed command %v is not an Op", m.Command)
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
