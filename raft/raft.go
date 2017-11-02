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
	"fmt"
	"labrpc"
	"math/rand"
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

	entries     []LogEntry // logs
	commitIndex int        // 已知被提交的最大日志条目索引
	lastApplied int        // 已知被状态机执行的最大日志条目索引

	// only for leader
	nextIndex  []int
	matchIndex []int

	timerChan chan bool
	exitChan  chan bool
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
	Term      int
	NextIndex int
	Logs      []LogEntry
}

type AppendEntriesReply struct {
	MatchIndex int
	Result     bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		rf.timerChan <- reply.Result
	}()
	// todo 需要生成matchIndex，并同步日志
	if args.Term > rf.term {
		rf.term = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
		reply.Result = true
	} else if args.Term == rf.term {
		reply.Result = true
	} else {
		reply.Result = false
		return
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Println("-args.term: ", args.Term, " args.Server: ", args.ServerIndex, " term: ", rf.term, " server: ", rf.me)
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		rf.timerChan <- reply.Result
	}()
	fmt.Println("---args.term: ", args.Term, " args.Server: ", args.ServerIndex, " term: ", rf.term, " server: ", rf.me)
	num := len(rf.entries)
	maxLogIndex := -1
	maxLogTerm := -1
	if num > 0 {
		maxLogIndex = rf.entries[num-1].Index
		maxLogTerm = rf.entries[num-1].Term
	}
	if args.Term > rf.term {
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
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.timerChan = make(chan bool)
	rf.exitChan = make(chan bool)

	for _ = range rf.peers {
		rf.nextIndex = append(rf.nextIndex, -1)
		rf.matchIndex = append(rf.matchIndex, -1)
	}

	rand.Seed(time.Now().UnixNano())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.startFollower()
	return rf
}

func (rf *Raft) startFollower() {
	fmt.Println("startFollow1 ---- ", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("startFollow2 -----", rf.me)
	rf.role = FOLLOWER
	rf.startFollowerLoop()
}

func (rf *Raft) startFollowerLoop() {

	ms := time.Duration(rand.Intn(300) + 300)
	go func() {
		t := time.NewTimer(ms * time.Millisecond)
		flag := 0
	ForEnd:
		for {
			select {
			case <-t.C:
				if flag > 0 {
					flag = 0
					ms := time.Duration(rand.Intn(300) + 300)
					t.Reset(ms * time.Millisecond)
				} else {
					rf.startElection()
					break ForEnd
				}
			case <-rf.timerChan:
				flag = 1
				//				t.Stop()
				//				select {
				//				case <-t.C:
				//				default:
				//				}
				//				ms = time.Duration(rand.Intn(400) + 600)
				//				t.Reset(ms * time.Millisecond)
			case <-rf.exitChan:
				t.Stop()
				break ForEnd
			}
		}
	}()
}

func (rf *Raft) startElection() {
	fmt.Println("startElec1 ----- ", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("startElec2 ----- ", rf.me)
	rf.term += 1
	rf.role = CANDIDATE
	rf.votedFor = rf.me
	// send RequestVoteRPC
	args := new(RequestVoteArgs)
	args.Term = rf.term
	args.ServerIndex = rf.me
	num := len(rf.entries)
	if num > 0 {
		args.LastLogTerm = rf.entries[num-1].Term
		args.LastLogIndex = rf.entries[num-1].Index
	} else {
		// 表示空日志
		args.LastLogTerm = -1
		args.LastLogIndex = -1
	}

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
		ms := time.Duration(rand.Intn(1000))
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

func (rf *Raft) getMaxLogIndex() int {
	num := len(rf.entries)
	if num == 0 {
		return -1
	} else {
		return rf.entries[num-1].Index
	}
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
	fmt.Println("startLeader1 ----- ", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("startLeader2 ----- ", rf.me)
	rf.role = LEADER
	nextIndex := rf.getMaxLogIndex()
	for i, _ := range rf.peers {
		rf.nextIndex[i] = nextIndex
		rf.matchIndex[i] = -1
	}
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
	args := new(AppendEntriesArgs)
	args.Term = rf.term
	go func() {
		for i, _ := range rf.peers {
			if i != rf.me {
				reply := new(AppendEntriesReply)
				rf.sendAppendEntries(i, args, reply)
			}
		}
	}()
}
