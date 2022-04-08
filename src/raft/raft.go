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
	"math/rand"
	"strings"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (st State) string() string {
	switch st {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return ""
}

//type AppendEntryType int
//
//const (
//	HeartBeat AppendEntryType = iota
//	Noop
//	Entry
//)

const NonVote = -1

const (
	electionTimeout  = 1000 * 1000 * 1000
	heartBeatTimeout = 1000 * 1000 * 1000
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	CurrentTerm int
	VotedFor    int

	State State

	lastHeartBeat   int64
	heartBeatCh     chan bool
	electionCh      chan bool
	appendEntriesCh chan interface{}
	successAppendCh chan bool
	ApplyCh         chan ApplyMsg
	//appendNoopCh    chan bool

	LogEntries []LogEntry
	matchIndex []int
	nextIndex  []int

	//leaderLastHeartBeatRespTime []int
	lastLeaderReply int64

	commitIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//func (rf *Raft) appendEntries(logEntries ...LogEntry) {
//	for _, entry := range logEntries {
//		rf.LogEntries = append(rf.LogEntries, LogEntry{
//			Term:    entry.Term,
//			Command: entry.Command,
//		})
//	}
//}

func (rf *Raft) leaseCheckTick() {
	for {
		if _, isLeader := rf.GetState(); isLeader {
			if time.Now().UnixNano()-rf.lastLeaderReply > 1000*1000*1000 {
				rf.State = Follower
			}
		}
	}
}

//func (rf *Raft) electionTick() {
//	for {
//		if _, isLeader := rf.GetState(); !isLeader {
//			rf.electionCh <- true
//			time.Sleep(time.Millisecond * time.Duration(rand.Intn(2000)))
//		}
//	}
//}

func (rf *Raft) heartBeatTick() {
	//	time.Sleep(time.Millisecond * time.Duration(3000))
	for {
		if _, isLeader := rf.GetState(); isLeader {
			rf.heartBeatCh <- true
			time.Sleep(100)
		}
	}
}

func (rf *Raft) electionTick() {
	//	time.Sleep(time.Millisecond * time.Duration(3000))
	for {
		if _, isLeader := rf.GetState(); !isLeader {
			elapseTime := time.Now().UnixNano() - rf.lastHeartBeat
			if elapseTime > 1000*1000 {
				rf.electionCh <- true
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(2000)))

			}
		}
	}
}

//func (rf *Raft) checkLeader() {
//	now := int(time.Now().UnixNano())
//	var count int
//	for i,_ := range rf.peers {
//		if i == rf.me {
//			continue
//		}
//		if (now - rf.leaderLastHeartBeatRespTime[i]) < 1500 * 1000 * 1000 {
//			count++
//		}
//	}
//	if count <= len(rf.peers)/2 {
//		rf.turnTo(Follower)
//
//	}
//}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	return rf.CurrentTerm, rf.State == Leader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// candidate's term.
	Term int
	// candidate requesting vote.
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// currentTerm, for candidate to update itself.
	Term int
	// true means candidate received vote.
	VoteGranted bool
}

type AppendEntriesRPC struct {
	// leader's term.
	Term int
	// leader's id, so follow can redirect client.
	LeaderId int
	// index of log entry immediately preceding new ones.
	PrevLogIndex int
	// term of preLogIndex entry.
	PrevLogTerm int
	// log entries to store.
	Entries []LogEntry
	// leader's commitIndex
	LeaderCommit int

	Mark bool

	//Type AppendEntryType
}

type AppendEntriesResponse struct {
	// currentTerm, for leader to update itself.
	Term int
	// true if follower contained entry matching
	// prevLogIndex and preLogTerm.
	Success bool

	ExpiredLeader bool
}

func (rf *Raft) HeartBeat(req *AppendEntriesRPC, resp *AppendEntriesResponse) {

	//rf.turnTo(Follower)
	now := time.Now().UnixNano()
	if rf.lastHeartBeat < now {
		rf.lastHeartBeat = now
	}
	//resp.Success = false
	//resp.Term = rf.CurrentTerm
	//
	//if req.Term < rf.CurrentTerm {
	//	resp.Term = rf.CurrentTerm
	//}
	//
	//if req.Term > rf.CurrentTerm {
	//	rf.CurrentTerm = req.Term
	//}
	//
	//if rf.lastLogIndex() >= req.LeaderCommit {
	//	resp.Success = true
	//	rf.commitIndex = req.LeaderCommit
	//}
	//applyStart := rf.commitIndex
	//
	//for i := applyStart; i <= rf.commitIndex; i++ {
	//	//fmt.Printf("peer[%d] apply logEntries[%d] command(%v)\n",rf.me,i,rf.LogEntries[i].Command)
	//	go rf.Apply(i, rf.LogEntries[i].Command)
	//}

}

func (rf *Raft) Apply(index int, cmd interface{}) {
	fmt.Printf("peer[%d] apply logEntries[%d] command(%v)\n", rf.me, index, rf.LogEntries[index].Command)
	rf.ApplyCh <- ApplyMsg{
		CommandValid: true,
		Command:      cmd,
		CommandIndex: index,
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

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesRPC, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(cmd interface{}) (int, int, bool) {
	if _, isLeader := rf.GetState(); !isLeader {
		return -1, -1, false
	}

	rf.LogEntries = append(rf.LogEntries, LogEntry{
		Term:    rf.CurrentTerm,
		Command: cmd,
	})

	//rf.appendEntries(LogEntry{Term: rf.CurrentTerm, Command: cmd})

	term := rf.CurrentTerm
	isLeader := true
	index := rf.lastLogIndex()

	// Your code here (2B).
	rf.appendEntriesCh <- cmd

	//timer := time.NewTimer(100 * time.Second)
	//
	//select {
	//case <-rf.successAppendCh:
	//	break
	//
	//case <-timer.C:
	//
	//}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rf.turnTo(Follower)
	//DPrintf("kill peer[%d]", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) turnTo(state State) {
	rf.State = state
}

func (rf *Raft) BroadcastHeartBeat() {

	if rf.commitIndex == 0 {
		//go rf.Apply(0, rf.LogEntries[0].Command)
		go rf.Apply(1, rf.LogEntries[1].Command)
		rf.commitIndex = 1
	}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(rf *Raft, i int) {

			//request := &AppendEntriesRPC{
			//	Term:         rf.CurrentTerm,
			//	LeaderId:     rf.me,
			//	LeaderCommit: rf.commitIndex,
			//}
			request := rf.newAppendEntriesRPC(-1, rf.commitIndex, true)
			response := &AppendEntriesResponse{}

			//	fmt.Printf("sendHeartBeat,request:%v\n", request)
			//if len(request.Entries) > 0 {
			//	str := fmt.Sprintf("%v", request.Entries[0].Command)
			//	if strings.Contains(str, "102") {
			//		fmt.Printf("sendHeartBeat,request:%v\n", request)
			//	}
			//}

			//fmt.Printf("sendHeartBeat,request:%v\n", request)
			//ch := make(chan Operation)

			if rf.sendHeartBeat(i, request, response) {
				now := time.Now().UnixNano()
				if now > rf.lastLeaderReply {
					rf.lastLeaderReply = now
				}
			}
		}(rf, i)
	}
}

func (rf *Raft) newAppendEntriesRPC(peer int, index int, toEnd bool) *AppendEntriesRPC {
	if index == 0 {
		//	panic("why index = 0?")
	}

	var entries []LogEntry
	if !toEnd {
		entries = []LogEntry{
			{
				Term:    rf.LogEntries[index].Term,
				Command: rf.LogEntries[index].Command,
			},
		}
	} else {
		entries = rf.LogEntries[index:]
	}

	preIndex := index - 1
	return &AppendEntriesRPC{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: preIndex,
		PrevLogTerm:  rf.LogEntries[preIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

type Operation struct {
	Peer int
	Left bool
	//right   bool
	Success bool
	RpcFail bool
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.LogEntries) - 1
}

func (rf *Raft) AppendEntries(req *AppendEntriesRPC, resp *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp.Success = false
	resp.Term = rf.CurrentTerm
	resp.ExpiredLeader = false

	if req.Mark {
		fmt.Printf("got 102+++++++++++++++++,return true\n")
		fmt.Printf("peer[%d] rf.logEntries=%v, rf.commitIndex=%d\n", rf.me, rf.LogEntries, rf.commitIndex)
		fmt.Printf("req.preIndex=%d,req.Entries=%v\n", req.PrevLogIndex, req.Entries)
	}

	if req.Term < rf.CurrentTerm {
		resp.Term = rf.CurrentTerm
		resp.ExpiredLeader = true
		return
	}

	if req.Term > rf.CurrentTerm {
		rf.CurrentTerm = req.Term
	}

	rf.HeartBeat(req, resp)

	if req.PrevLogIndex > rf.lastLogIndex() {
		return
	}
	if rf.LogEntries[req.PrevLogIndex].Term != req.PrevLogTerm {
		return
	}

	//if req.PrevLogIndex == -1 {
	//	resp.Success = true
	//	rf.commitIndex = req.LeaderCommit
	//}
	originCommitIndex := rf.commitIndex

	rf.LogEntries = rf.LogEntries[:req.PrevLogIndex+1]
	rf.LogEntries = append(rf.LogEntries, req.Entries...)

	//if rf.commitIndex == req.LeaderCommit {
	//	resp.Success = true
	//	resp.NoMoreApply = true
	//	return
	//}

	resp.Success = true
	rf.commitIndex = req.LeaderCommit

	//if len(req.Entries) > 0 {
	//	str := fmt.Sprintf("%v", req.Entries[0].Command)
	//	if strings.Contains(str, "102") {
	//		fmt.Printf("follower[%d] deal with 102\n", rf.me)
	//	}
	//}

	if originCommitIndex < rf.commitIndex {
		for i := originCommitIndex + 1; i <= rf.commitIndex; i++ {
			//fmt.Printf("peer[%d] apply logEntries[%d] command(%v)\n",rf.me,i,rf.LogEntries[i].Command)
			rf.Apply(i, rf.LogEntries[i].Command)
		}
		//resp.NextIndex = rf.lastLogIndex() + 1
	}

	//if rf.lastLogIndex() < req.LeaderCommit {
	//	rf.commitIndex = rf.lastLogIndex()
	//}else{
	//
	//}

	//for i, entry := range rf.LogEntries {
	//	fmt.Printf("peer[%d] logEntries[%d] command(%v)\n", rf.me, i, entry.Command)
	//}

}

func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesRPC, resp *AppendEntriesResponse, ch chan Operation) bool {

	startTime := time.Now().UnixNano()

	ok := rf.peers[server].Call("Raft.AppendEntries", req, resp)

	endTime := time.Now().UnixNano()
	if len(req.Entries) > 0 {
		str := fmt.Sprintf("%v", req.Entries[0].Command)
		if strings.Contains(str, "102") {
			fmt.Printf("To [%d] appendentries 102 time:%d\n", server, (endTime-startTime)/1000/1000/1000)
		}
	}

	fmt.Printf("To [%d] appendentries time:%d\n", server, (endTime-startTime)/1000/1000/1000)
	//
	//for {
	//	elapseTime := time.Now().UnixNano()
	//	if elapseTime-startTime > 100*1000*1000 {
	//
	//	}
	//}
	//if len(req.Entries) > 0 {
	//	str := fmt.Sprintf("%v", req.Entries[0].Command)
	//	if strings.Contains(str, "102") {
	//		fmt.Printf("sendAppendEntries deal with 102\n")
	//	}
	//}
	if ok {
		if resp.Success {
			ch <- Operation{Peer: server, Success: true}
			//if resp.NextIndex != -1 {
			//	rf.nextIndex[server] = resp.NextIndex
			//}
		} else if resp.Term > rf.CurrentTerm {
			rf.CurrentTerm = resp.Term
			rf.turnTo(Follower)
			//fmt.Printf("________turn to follower_______")
		} else if resp.ExpiredLeader {

		} else {
			ch <- Operation{Peer: server, Left: true}
		}
	} else {
		ch <- Operation{Peer: server, RpcFail: true}
	}

	return ok
}

func (rf *Raft) resetNextIndex(index int) {
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = index
	}
}

func (rf *Raft) BroadcastAppendEntries(cmd interface{}) {
	//fmt.Printf("BroadcastAppendEntries(): Command=%v,commitIndex=%d\n", cmd, rf.commitIndex)

	rf.resetNextIndex(rf.lastLogIndex())

	//fmt.Printf("nextIndex: %d\n", rf.lastLogIndex())

	//for i, entry := range rf.LogEntries {
	//	fmt.Printf("peer[%d] logEntries[%d] command(%v)\n", rf.me, i, entry.Command)
	//}

	operationCh := make(chan Operation)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(rf *Raft, i int, ch chan Operation) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			req := rf.newAppendEntriesRPC(i, rf.lastLogIndex(), false)
			if len(req.Entries) > 0 {
				str := fmt.Sprintf("%v", req.Entries[0].Command)
				if strings.Contains(str, "102") {
					req.Mark = true
					fmt.Printf("BroadcastAppendEntries():sendAppendEntriesRPC()+++marked+++ to peer[%d]:%v\n", i, req)
				}
			}
			resp := &AppendEntriesResponse{}

			rf.sendAppendEntries(i, req, resp, ch)
			//fmt.Printf("~~~~~~to peer[%d],ok=%v\n", i, ok)

			//if ok {
			//	if resp.Success {
			//		//if len(req.Entries) > 0 {
			//		//	str := fmt.Sprintf("%v", req.Entries[0].Command)
			//		//	if strings.Contains(str, "102") {
			//		//		fmt.Printf("peer[%d] appendentries 102 success\n", i)
			//		//	}
			//		//}
			//		rf.nextIndex[i] = rf.commitIndex + 1
			//		*ch <- true
			//	} else if resp.Term > rf.CurrentTerm {
			//		rf.CurrentTerm = resp.Term
			//		rf.turnTo(Follower)
			//	} else if resp.ExpiredLeader {
			//
			//	} else {
			//		rf.nextIndex[i]--
			//		go rf.sendAppendEntries(
			//			i,
			//			rf.newAppendEntriesRPC(-1, rf.nextIndex[i], true),
			//			&AppendEntriesResponse{},
			//		)
			//	}
			//} else {
			//fmt.Printf("rpc fail ++++++\n")
			//rpc fail
			//go rf.sendAppendEntries(
			//	i,
			//	rf.newAppendEntriesRPC(-1, rf.nextIndex[i], true),
			//	&AppendEntriesResponse{},
			//)
			//}

		}(rf, i, operationCh)

		successCount := 1
		for {
			if _, isLeader := rf.GetState(); !isLeader {
				break
			}
			if successCount > len(rf.peers)/2 {
				applyStart := rf.commitIndex
				rf.commitIndex = rf.lastLogIndex()
				for j := applyStart; j <= rf.commitIndex; j++ {
					rf.Apply(j, rf.LogEntries[j].Command)
				}
				rf.heartBeatCh <- true
				break
			}
			//timer := time.NewTimer(100 * 1000 * 1000)
			select {
			case op := <-operationCh:

				if op.Left {
					rf.nextIndex[op.Peer]--
					go rf.sendAppendEntries(
						op.Peer,
						rf.newAppendEntriesRPC(op.Peer, rf.nextIndex[op.Peer], true),
						&AppendEntriesResponse{},
						operationCh,
					)
				}
				if op.Success {

					rf.nextIndex[op.Peer] = rf.commitIndex + 1
					successCount++
					fmt.Printf("success++ = %d,command=%v\n", successCount, cmd)
				}
				if op.RpcFail {
					go rf.sendAppendEntries(
						op.Peer,
						rf.newAppendEntriesRPC(op.Peer, rf.nextIndex[op.Peer], false),
						&AppendEntriesResponse{},
						operationCh,
					)
				}
				//case <-timer.C:
				//	rf.appendEntriesCh <- cmd
				//	return
			}
		}
	}

}

func (rf *Raft) lastLogTerm() int {
	return rf.LogEntries[rf.lastLogIndex()].Term
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(request *RequestVoteArgs, response *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if request.Term > rf.CurrentTerm || (request.Term == rf.CurrentTerm && rf.VotedFor != -1) {
		if request.LastLogTerm < rf.lastLogTerm() || (request.LastLogTerm == rf.lastLogTerm() && request.LastLogIndex < rf.lastLogIndex()) {
			response.Term, response.VoteGranted = rf.CurrentTerm, false
			DPrintf("RequestVote(): (peer[%d],currentTerm[%d], lastLogTerm[%d], lastLogIndex[%d]) reject (peer[%d],voteTerm[%d],lastLogTerm[%d],lastLogIndex[%d],)\n",
				rf.me, rf.CurrentTerm, rf.lastLogTerm(), rf.lastLogIndex(), request.CandidateId, request.Term, request.LastLogTerm, request.LastLogIndex)
			return
		}
		rf.turnTo(Follower)
		rf.CurrentTerm, rf.VotedFor = request.Term, request.CandidateId
		response.Term, response.VoteGranted = rf.CurrentTerm, true

		DPrintf("RequestVote(): peer[%d] granted from (peer[%d],term[%d]) change to CurrentTerm= [%d]",
			rf.me, request.CandidateId, request.Term, rf.CurrentTerm)
		return
	}

	if request.Term < rf.CurrentTerm {
		response.Term, response.VoteGranted = rf.CurrentTerm, false

		DPrintf("RequestVote(): (peer[%d],currentTerm[%d]) reject from (peer[%d],voteTerm[%d])",
			rf.me, rf.CurrentTerm, request.CandidateId, request.Term)
		return

	}
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.turnTo(Candidate)
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	grantedVotes := 1
	rf.mu.Unlock()
	var wg sync.WaitGroup

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int, rf *Raft) {
			request := &RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastLogIndex(),
				LastLogTerm:  rf.lastLogTerm(),
			}
			response := &RequestVoteReply{}

			if rf.sendRequestVote(i, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.CurrentTerm == request.Term && rf.State == Candidate {
					if response.VoteGranted && response.Term == request.Term {
						grantedVotes++
						DPrintf("startElection(): Candidate [%d] sendRequestVote Term=%d , grantedVotes = %d \n", rf.me, request.Term, grantedVotes)
					}

					if response.Term > rf.CurrentTerm {
						rf.turnTo(Follower)
						rf.CurrentTerm, rf.VotedFor = response.Term, -1
						return
					}

					if grantedVotes > len(rf.peers)/2 {
						rf.turnTo(Leader)
						DPrintf("StartElection(): peer[%d] thinks new leader is [%d], currentTerm is [%d], grantedVotes are %d\n",
							rf.me, request.CandidateId, rf.CurrentTerm, grantedVotes)
						rf.lastLeaderReply = time.Now().UnixNano()

						rf.heartBeatCh <- true
						//rf.appendEntriesCh <- "noop"
					}
				}

			}
			wg.Done()
		}(i, rf)
	}

	wg.Wait()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionCh:

			// DPrintf("StartElection\n")
			go rf.StartElection()

		case <-rf.heartBeatCh:
			go rf.BroadcastHeartBeat()

		case cmd := <-rf.appendEntriesCh:
			go rf.BroadcastAppendEntries(cmd)

		}

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
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
	rf.State = Follower
	rf.commitIndex = 0
	rf.LogEntries = make([]LogEntry, 2)
	rf.electionCh = make(chan bool)
	rf.heartBeatCh = make(chan bool)
	rf.appendEntriesCh = make(chan interface{})

	rf.ApplyCh = applyCh

	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	// Your initialization code here (2A, 2B, 2C).
	go rf.electionTick()
	go rf.heartBeatTick()
	go rf.leaseCheckTick()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
