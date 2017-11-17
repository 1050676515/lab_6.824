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

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

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

const (
	FOLLOWER int = iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Term  int
	Index int
	Cmd   interface{}
	Type  int // 0 internal | 1 command
	AppId int
}

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

	// 这三个要同时变化
	term     int // currentTerm
	role     int // role
	votedFor int // candidateId in current term
	leader   int // leader

	entries     []LogEntry // logs
	commitIndex int        // 已知被提交的最大日志条目索引
	lastApplied int        // 已知被状态机执行的最大日志条目索引
	logAppId    int

	// only for leader
	nextIndex  []int
	matchIndex []int

	timerChan chan bool
	exitChan  chan bool
	applyCh   chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isleader = rf.role == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.entries)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.lastApplied)
	d.Decode(&rf.term)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.entries)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LastLogTerm  int
	LastLogIndex int
	ServerIndex  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Result bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Logs         []LogEntry
}

type AppendEntriesReply struct {
	Success bool
}

func (rf *Raft) checkLogConsistency(prevTerm, prevIndex int) bool {
	maxLogTerm, maxLogIndex := rf.getMaxLog()
	if maxLogTerm >= prevTerm && maxLogIndex >= prevIndex && rf.entries[prevIndex].Term == prevTerm && rf.entries[prevIndex].Index == prevIndex {
		rf.entries = rf.entries[:prevIndex+1]
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	result := false
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		rf.timerChan <- result
	}()

	// 同步日志
	if args.Term >= rf.term {
		result = true
		if args.Term > rf.term {
			// term增加后，必须重置role为FOLLOWER
			rf.term = args.Term
			rf.votedFor = -1
			rf.role = FOLLOWER
		}

		rf.leader = args.LeaderId
		if rf.checkLogConsistency(args.PrevLogTerm, args.PrevLogIndex) {
			rf.entries = append(rf.entries, args.Logs...)
			for oldCommitIndex := rf.commitIndex + 1; oldCommitIndex <= args.LeaderCommit; oldCommitIndex++ {
				if rf.entries[oldCommitIndex].Type == 1 {
					rf.applyCh <- ApplyMsg{Index: rf.entries[oldCommitIndex].AppId, Command: rf.entries[oldCommitIndex].Cmd}
				}
				rf.commitIndex += 1
				rf.lastApplied += 1
			}
			reply.Success = true
		} else {
			reply.Success = false
		}
		rf.persist()
	} else {
		reply.Success = false
		result = false
	}
	//	fmt.Println("server: ", rf.me, " logs: ", rf.entries, " commitIndex: ", rf.commitIndex, " term: ", rf.term, " args.PrevLogIndex: ", args.PrevLogIndex)
	//	fmt.Println("server: ", rf.me, " args.PrevLogIndex: ", args.PrevLogIndex, " lastApplied: ", rf.lastApplied, " logs: ", rf.entries)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		rf.timerChan <- reply.Result
	}()
	maxLogTerm, maxLogIndex := rf.getMaxLog()

	//	fmt.Println("server: ", rf.me, " maxLogTerm: ", maxLogTerm, " maxLogIndex: ", maxLogIndex, " term: ", rf.term, " args.Server: ", args.ServerIndex, " args.Term: ", args.Term, " args.LastLogTerm: ", args.LastLogTerm, " args.LastLogIndex: ", args.LastLogIndex)
	if args.Term > rf.term {
		// term增加后，必须重置role为FOLLOWER
		rf.term = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
	} else if args.Term == rf.term {
		// 已投过票
		if rf.votedFor != -1 || rf.role == LEADER {
			reply.Result = false
			return
		}
	} else {
		reply.Result = false
		return
	}

	if args.LastLogTerm > maxLogTerm {
		// 给别人投票，转化自己为FOLLOWER
		rf.role = FOLLOWER
		rf.votedFor = args.ServerIndex
		reply.Result = true
	} else if args.LastLogTerm == maxLogTerm && args.LastLogIndex >= maxLogIndex {
		rf.votedFor = args.ServerIndex
		rf.role = FOLLOWER
		reply.Result = true
	} else {
		reply.Result = false
	}
	rf.persist()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = len(rf.entries)
	appId := rf.getMaxAppId() + 1
	term = rf.term
	if rf.role == LEADER {
		isLeader = true
		entry := LogEntry{Term: term, Index: index, Cmd: command, AppId: appId, Type: 1}
		rf.entries = append(rf.entries, entry)
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.persist()
	} else {
		isLeader = false
	}
	return appId, term, isLeader
}

func (rf *Raft) getMaxAppId() int {
	for i := len(rf.entries) - 1; i >= 0; i-- {
		if rf.entries[i].Type == 1 {
			return rf.entries[i].AppId
		}
	}

	return 0
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.exitChan <- true
	close(rf.exitChan)
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.term = 0
	rf.role = FOLLOWER
	rf.votedFor = -1
	rf.leader = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.entries = append(rf.entries, LogEntry{Term: 0, Index: 0, Cmd: 0, Type: 0})
	rf.timerChan = make(chan bool)
	rf.exitChan = make(chan bool)
	rf.applyCh = applyCh
	rf.applyCh <- ApplyMsg{Index: 0, Command: 0}

	for _ = range rf.peers {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rand.Seed(time.Now().UnixNano())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.startFollower()
	return rf
}

func (rf *Raft) startFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = FOLLOWER
	rf.startFollowerLoop()
}

func (rf *Raft) startFollowerLoop() {

	ms := time.Duration(rand.Intn(1000) + 500)
	go func() {
		t := time.NewTimer(ms * time.Millisecond)
		//		flag := 0
	ForEnd:
		for {
			select {
			case <-t.C:
				//				if flag > 0 {
				//					flag = 0
				//					ms := time.Duration(rand.Intn(300) + 300)
				//					t.Reset(ms * time.Millisecond)
				//				} else {
				//					rf.startElection()
				//					break ForEnd
				//				}
				rf.startElection()
				break ForEnd
			case res := <-rf.timerChan:
				//				flag = 1
				if res {
					t.Stop()
					select {
					case <-t.C:
					default:
					}
					ms = time.Duration(rand.Intn(1000) + 500)
					t.Reset(ms * time.Millisecond)
				}
			case <-rf.exitChan:
				t.Stop()
				break ForEnd
			}
		}
	}()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.term += 1
	rf.role = CANDIDATE
	rf.leader = -1
	rf.votedFor = rf.me
	rf.persist()
	// send RequestVoteRPC
	args := new(RequestVoteArgs)
	args.Term = rf.term
	args.ServerIndex = rf.me
	args.LastLogTerm, args.LastLogIndex = rf.getMaxLog()

	voteChan := make(chan int, 1)
	// 给所有服务器发送RequestVoteRPC
	go func(voteChan chan<- int, needVote, term int) {
		var vote int32
		vote = 1
		var wg sync.WaitGroup
		//		start := time.Now().UnixNano()
		for index, _ := range rf.peers {
			if index != rf.me {
				wg.Add(1)
				go func(i int) {
					reply := new(RequestVoteReply)
					ok := rf.sendRequestVote(i, args, reply)
					if ok && reply.Result {
						v := atomic.AddInt32(&vote, 1)
						if v == int32(needVote) {
							voteChan <- term
						}
					}
					wg.Done()
				}(index)
			}
		}
		ch := make(chan bool, 1)
		go func() {
			wg.Wait()
			ch <- true
		}()
		ms := time.Duration(rand.Intn(1000) + 500)
		t := time.NewTimer(ms * time.Millisecond)
		defer t.Stop()
	ForEnd:
		for {
			select {
			case <-ch:
				if vote >= int32(needVote) {
					break ForEnd
				}
			case <-t.C:
				voteChan <- -1
			}
		}
		//		wg.Wait()
		//		end := time.Now().UnixNano()
		//		rf.mu.Lock()
		//		defer rf.mu.Unlock()
		//		if rf.term == term && rf.role == CANDIDATE && vote >= int32(needVote) {
		//			voteChan <- term
		//		} else {
		//		fmt.Println("---time sum: ", (end-start)/1e6)
		//		if vote < int32(needVote) {
		//			fmt.Println("--------------------------------------------time sum1: ", (end-start)/1e6)
		//			ms = ms - (end-start)/1e6
		//			if ms < 0 {
		//				ms = 0
		//			}
		//			time.Sleep(time.Duration(ms) * time.Millisecond)
		//			fmt.Println("----------------------------------------------newelec1")
		//			voteChan <- -1
		//			fmt.Println("----------------------------------------------newelec2")
		//		}
	}(voteChan, len(rf.peers)/2+1, rf.term)
	rf.startElectionLoop(voteChan)
}

func (rf *Raft) getMaxLog() (int, int) {
	num := len(rf.entries)
	if num == 0 {
		return -1, -1
	} else {
		return rf.entries[num-1].Term, rf.entries[num-1].Index
	}
}

func (rf *Raft) getMinMatchIndex() int {
	var matchs []int
	matchs = append(matchs, rf.matchIndex...)
	sort.Sort(sort.Reverse(sort.IntSlice(matchs)))
	needNum := len(rf.peers)/2 + 1
	return matchs[needNum-1]
}

func (rf *Raft) startElectionLoop(voteChan <-chan int) {
	go func() {
	ForEnd:
		for {
			select {
			case <-rf.exitChan:
				break ForEnd
			case term := <-voteChan:
				rf.mu.Lock()
				if term == rf.term && rf.role == CANDIDATE {
					// 当选成功
					rf.mu.Unlock()
					rf.startLeader()
					break ForEnd
				} else if term == -1 {
					rf.mu.Unlock()
					rf.startElection()
					break ForEnd
				} else {
					rf.mu.Unlock()
				}
			case res := <-rf.timerChan:
				// 选举过期
				if res {
					// 转换为follower
					rf.startFollower()
					break ForEnd
				}
			}
		}
	}()
}

func (rf *Raft) startLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = LEADER
	rf.leader = rf.me
	_, maxIndex := rf.getMaxLog()
	maxIndex += 1
	// Raft 中通过领导人在任期开始的时候提交一个空白的没有任何操作的日志条目到日志中去来进行实现
	entry := LogEntry{Term: rf.term, Index: maxIndex, Cmd: 0, Type: 0}
	rf.entries = append(rf.entries, entry)

	for i, _ := range rf.peers {
		rf.nextIndex[i] = maxIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = maxIndex
	rf.startLeaderLoop()
}

func (rf *Raft) startLeaderLoop() {
	go func() {
		rf.sendHeartBeat()
		t := time.NewTimer(100 * time.Millisecond)
	ForEnd:
		for {
			select {
			case <-t.C:
				// 发送心跳
				rf.sendHeartBeat()
				t.Reset(100 * time.Millisecond)
			case <-rf.exitChan:
				t.Stop()
				break ForEnd
			case res := <-rf.timerChan:
				// 领导人过期
				if res {
					t.Stop()
					rf.startFollower()
					break ForEnd
				}
			}
		}
	}()
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return
	}
	minMatch := rf.getMinMatchIndex()
	// S1在时序(c)的任期term4提交term2的旧日志时，旧日志必须附带在当前term 4的日志下一起提交
	/*
		Raft never commits log entries from previous terms by counting replicas. Only log entries from the leader's current term are committed by counting replicas; once an entry from the current term has been committed in this way, then all prior entries are committed indirectly because of the Log Matching Property.
	*/
	if rf.entries[minMatch].Term == rf.term {
		for oldCommitIndex := rf.commitIndex + 1; oldCommitIndex <= minMatch; oldCommitIndex++ {
			if rf.entries[oldCommitIndex].Type == 1 {
				rf.applyCh <- ApplyMsg{Index: rf.entries[oldCommitIndex].AppId, Command: rf.entries[oldCommitIndex].Cmd}
			}
			rf.commitIndex += 1
			rf.lastApplied += 1
		}
		rf.persist()
	}
	//	fmt.Println("leader: ", rf.me, " logs: ", rf.entries, " commitIndex: ", rf.commitIndex, " term: ", rf.term)
	//	fmt.Println("leader: ", rf.me, " commitIndex: ", rf.commitIndex, " term: ", rf.term, " lastApplied: ", rf.lastApplied, " logs: ", rf.entries)

	for i, _ := range rf.peers {
		if i != rf.me {
			go func(index, term int) {
				rf.mu.Lock()
				args := new(AppendEntriesArgs)
				args.Term = term
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = rf.nextIndex[index] - 1
				nextIndex := len(rf.entries)
				if args.PrevLogIndex > 0 {
					args.PrevLogTerm = rf.entries[args.PrevLogIndex].Term
					args.Logs = rf.entries[rf.nextIndex[index]:]
				} else {
					args.PrevLogTerm = rf.entries[0].Term
					args.Logs = rf.entries[1:]
				}
				rf.mu.Unlock()
				reply := new(AppendEntriesReply)
				reply.Success = false
				rf.sendAppendEntries(index, args, reply)
				if reply.Success {
					rf.mu.Lock()
					rf.nextIndex[index] = nextIndex
					rf.matchIndex[index] = nextIndex - 1
					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					if rf.nextIndex[index] > 1 {
						rf.nextIndex[index] = rf.nextIndex[index] / 2
					} else {
						rf.nextIndex[index] = 1
					}
					rf.mu.Unlock()
				}
			}(i, rf.term)
		}
	}
}
