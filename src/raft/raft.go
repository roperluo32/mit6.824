package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type logEntry struct {
	term  int
	value int
}

type State int

//state
const (
	Follower  State = 0 //0
	Candidate State = 1 //1
	Leader    State = 2 //2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	recvHeartBeatTime int64      //上一次收到心跳的时间
	heartBeatTimeout  int        //心跳超时，每次都重新随机生成（150～300ms）
	randGen           *rand.Rand //随机数生成器
	currentTerm       int
	voteFor           int
	log               []logEntry
	state             State

	commitIndex int //提交的最新的日志index
	lastApplied int //应用到状态即的最新日志index

	//leader专属字段
	nextIndex   []int
	matchIndex  []int
	sendLogTime int64 //发送日志的时间

	//candidate相关字段
	votedForMe      []int //记录有哪些人给我投票了
	voteTime        int64 //选举计时器
	maxVoteWaitTime int   //最大的选举等待时间，每次开始选举时从150～300ms中随机选择一个时间

	//debug
	debugSwitch bool
}

//添加日志/心跳包结构体
type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	//	Entries      []logEntry //保存发给follower的日志条目，心跳包为0
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success int
}

// return currentTerm and whether this server
// believes it is the leader.
//返回当前的term以及自己是否是leader信息
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
//保存持久化信息，当raft宕机重启后使用
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
//读取持久化信息
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
//请求投票包
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
//请求投票回复包
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted int
}

//
// example RequestVote RPC handler.
//
//处理请求投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.DPrintf("raft %v receive request vote from raft %v, hist term:%v, mine:%v", rf.me, args.CandidateId, args.Term, rf.currentTerm)

	//更新时间戳
	rf.recvHeartBeatTime = getMillisTime()

	//请求投票的term大于自己的term，无条件给它投票，自己转为追随者
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		reply.VoteGranted = 1
		rf.voteFor = args.CandidateId

		rf.DPrintf("raft %v vote for him:%v", rf.me, args.CandidateId)
	} else if args.Term < rf.currentTerm {
		//投票的term小于自己的，不给它投票
		reply.Term = rf.currentTerm
		reply.VoteGranted = 0

		rf.DPrintf("me:%v, src:%v, RequestVote his term:%v lower than mine:%v", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	} else if args.Term == rf.currentTerm {
		//已经给别人投票了
		if rf.voteFor != 0 {
			reply.VoteGranted = 0
			rf.DPrintf("me:%v, src:%v, I have voted for %v", rf.me, args.CandidateId, rf.voteFor)
		} else {
			//args的日志索引小于我的，不投票
			if args.LastLogIndex < rf.commitIndex {
				reply.VoteGranted = 0
				rf.DPrintf("me:%v, src:%v, his index:%v is lower than mine:%v", rf.me, args.CandidateId, args.LastLogIndex, rf.commitIndex)
			} else {
				//给请求者投票
				reply.VoteGranted = 1
				rf.voteFor = args.CandidateId
			}
		}
	}

	reply.Term = rf.currentTerm

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
//发送请求投票
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if ok {
		rf.DPrintf("raft %v receive vote reply from raft %v. reply: %v", rf.me, server, reply)
		//回复包的term比自己的要大，变成追随者
		if reply.Term > rf.currentTerm {
			rf.DPrintf("me:%v, src:%v, vote reply's term:%v bigger than me:%v. I'll be follower:%v", rf.me, server, reply.Term, rf.currentTerm)
			rf.state = Follower
			return ok
		}

		//回复包的term比自己小，丢弃该回复
		if reply.Term < rf.currentTerm {
			rf.DPrintf("RequestVote reply's term:%v is too small(mine:%v), discard...", reply.Term, rf.currentTerm)
			return false
		}

		//为自己投票了
		if reply.VoteGranted == 1 {
			rf.votedForMe[server] = 1
		}

		//计算目前的得票数是否超过了半数
		count := 0
		for i := range rf.votedForMe {
			if rf.votedForMe[i] == 1 {
				count += 1
			}
		}

		rf.DPrintf("me:%v, Now I have get %v votes.", rf.me, count)

		//如果超过了半数，那么就变成领导人
		if count >= len(rf.peers)/2+1 {
			rf.state = Leader
			rf.DPrintf("me:%v,I'll be LEADER term:%v~~~", rf.me, rf.currentTerm)
		}

	}

	return ok
}

//发送添加日志请求
func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	//收到的reply的term比我的大
	if reply.Term > rf.currentTerm {
		rf.DPrintf("me:%v, src:%v, recv append reply. his term:%v is bigger than me:%v. I'll change to follower", rf.me, server, reply.Term, rf.currentTerm)
		rf.state = Follower
	}

	return ok
}

//发送心跳
func (rf *Raft) sendHeartBeat(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func getMillisTime() int64 {
	now := time.Now()
	nanos := now.UnixNano()
	millis := nanos / 1000000

	return millis
}

//处理添加日志请求
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	//记录收到来自leader的添加日志请求的时间
	rf.recvHeartBeatTime = getMillisTime()

	rf.DPrintf("raft %v receive append log request from raft %v.his term:%v, my term:%v", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	reply.Success = 1
	if args.Term < rf.currentTerm {
		//leader的term比自己还小
		rf.DPrintf("me:%v, src:%v, AppendEntries his term:%v is lower than me:%v", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = 0
	} else if args.Term == rf.currentTerm {
		//我也是leader，收到了来自拥有相同term的leader的append entry。
		if rf.state == Leader {
			panic("[ERROR]Have two leader...")
		} else if rf.state == Candidate {
			//竞选者收到了另一个leader的添加日志请求
			rf.state = Follower
			rf.DPrintf("me:%v I'm  a candidate. receve append log from raft:%v, change to Follower", rf.me, args.LeaderId)
		}
	} else {
		rf.DPrintf("me:%v, src:%v, AppendEntries my term:%v is lower than leader:%v, my state %v change to Follower", rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.state)
		rf.currentTerm = args.Term
		rf.state = Follower
	}

	reply.Term = rf.currentTerm
}

//
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.debugSwitch = false
}

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	DPrintf(format, a...)
	return
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func (rf *Raft) startCandidate() {

	//变成竞争者状态
	rf.state = Candidate

	//将投票计数清0
	for i := range rf.votedForMe {
		rf.votedForMe[i] = 0
	}

	//将term + 1
	rf.currentTerm += 1
	rf.DPrintf("raft %v start to candidate.term: %v", rf.me, rf.currentTerm)

	//为自己投票
	rf.votedForMe[rf.me] = 1
	rf.voteFor = rf.me

	//选举计时器开始计时
	rf.voteTime = getMillisTime()

	//随机选择选举超时时间
	rf.maxVoteWaitTime = rf.getRandWaitTime()

	//给所有其它raft节点发送投票请求
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.commitIndex
	if rf.commitIndex >= 0 {
		args.LastLogTerm = rf.log[rf.commitIndex].term
	}
	reply := &RequestVoteReply{}

	for i := range rf.peers {
		if i != rf.me {
			rf.DPrintf("raft %v send request vote to raft %v. term:%v", rf.me, i, rf.currentTerm)
			go func(svr int) {
				rf.sendRequestVote(svr, args, reply)
			}(i)
		}
	}
}

//获取心跳超时和选举超时时间，设置为150～300ms中的一个随机数
func (rf *Raft) getRandWaitTime() int {
	return rf.randGen.Intn(1000) + 1000
}

func (rf *Raft) initParams() {
	//收到心跳的时间初试为当前时间
	rf.recvHeartBeatTime = getMillisTime()

	//随机生成
	rf.randGen = rand.New(rand.NewSource(rf.voteTime + int64(rf.me)))
	rf.heartBeatTimeout = rf.getRandWaitTime() //150~300 ms

	//初始状态为Follower
	rf.state = Follower

	//日志索引和应用的日志索引赋初始值为-1
	rf.commitIndex = -1
	rf.lastApplied = -1

	//初始化当前term为0
	rf.currentTerm = 0

	//投票相关字段清0
	npeers := len(rf.peers)
	rf.votedForMe = make([]int, npeers)
	for i := range rf.votedForMe {
		rf.votedForMe[i] = 0
	}
	rf.voteFor = 0
	rf.voteTime = 0

	rf.sendLogTime = 0

	//分配log
	rf.log = make([]logEntry, npeers)

	//分配
	rf.nextIndex = make([]int, npeers)
	rf.matchIndex = make([]int, npeers)

	rf.debugSwitch = true

	//这里从persister中回复数据
	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())

}

func (rf *Raft) broadcastLog() {
	//给所有其它raft节点发送添加日志请求
	args := &AppendEntries{}

	args.Term = rf.currentTerm
	args.LeaderId = rf.me

	if rf.commitIndex > 0 {
		args.PrevLogIndex = rf.commitIndex - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].term
	} else {
		args.PrevLogIndex = -1
		args.PrevLogTerm = -1
	}

	/*
		args.Entries = make([]logEntry, 1)

		if rf.commitIndex > 0 {
			args.Entries[0] = rf.log[rf.commitIndex]
		}
	*/

	args.LeaderCommit = rf.lastApplied

	reply := &AppendEntriesReply{}
	//	rf.DPrintf("raft %v send append log.", rf.me)
	for i := range rf.peers {
		if i != rf.me {
			rf.DPrintf("raft %v send append log to raft %v", rf.me, i)
			go func(svr int) {
				rf.sendAppendEntries(svr, args, reply)
			}(i)
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	//初始化raft的参数
	rf.initParams()

	// Your initialization code here (2A, 2B, 2C).
	//创建一个协程监听leader的心跳，如果超过一定时间没有收到leader的心跳，那么就发出投票请求

	const SLEEP_INTERVAL = 20
	const SEND_LOG_INTERVAL = 100
	go func() {

		for {
			time.Sleep(SLEEP_INTERVAL)
			millisnow := getMillisTime()

			//如果自己是leader
			if rf.state == Leader {
				//定时发送心跳
				if rf.sendLogTime+SEND_LOG_INTERVAL < millisnow {
					rf.sendLogTime = getMillisTime()
					rf.broadcastLog()

				}
			} else if rf.state == Follower { //如果是follower
				//超过最长时限没有收到来自leader的心跳
				if rf.recvHeartBeatTime+int64(rf.heartBeatTimeout) < millisnow {

					rf.DPrintf("raft %v wait heartbeat timeout.time:%v, timeout:%v", rf.me, rf.recvHeartBeatTime, rf.heartBeatTimeout)

					//重新生成心跳超时时间
					rf.heartBeatTimeout = rf.getRandWaitTime()

					//开始election
					go rf.startCandidate()
				}
			} else if rf.state == Candidate { //如果是Candidate
				//检查是否选举超时
				if rf.voteTime+int64(rf.maxVoteWaitTime) < millisnow {

					rf.DPrintf("raft %v vote timeout", rf.me)
					// 重新生成选举超时时间
					rf.maxVoteWaitTime = rf.getRandWaitTime()

					go rf.startCandidate()
				}
			}
		}

	}()

	return rf
}
