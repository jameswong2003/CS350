package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cs350/labgob"
	"cs350/labrpc"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int

	currentTerm int
	votedFor    int

	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	applyCh chan ApplyMsg
	// Volatile state on all servers
	log         []*LogEntry
	LogInfo     LogEntry
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// Grab the last index of the log
func (rf *Raft) GetLastLogIndex() int {
	return len(rf.log) - 1
}

// Grab the last term of the last index of the log
func (rf *Raft) GetLastLogTerm() int {
	return rf.log[rf.GetLastLogIndex()].Term
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (4A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil {
		panic("failed to encode raft persistent state")
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil {
		panic("Failed to decode raft persistent state")
	}

}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int
	VoteGranted bool
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Check if argument term is greater
	if args.Term > rf.currentTerm {
		rf.convertTo(Follower)
		rf.currentTerm = args.Term
	}

	// Check if term is smaller or already voted
	if args.Term < rf.currentTerm || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// if votedfor is nulled or candidates id, and candidate's log is at least up to date as receiver's log, grant vote
	// LastLogIndex := rf.GetLastLogIndex()
	// LastLogTerm := rf.GetLastLogTerm()
	// upToDate := false

	// if args.LastLogTerm > LastLogTerm {
	// 	upToDate = true
	// }
	// if args.LastLogTerm == LastLogTerm {
	// 	if args.LastLogIndex > LastLogIndex {
	// 		upToDate = true
	// 	}
	// }

	// confirm vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	CommitIndex  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Update reply.Term atomically
	reply.Term = rf.currentTerm

	// Check if term < current term
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// Check if the log contains an entry at prevLogIndex whose term matches prevLogTerm
	// lastLogIndex := rf.GetLastLogIndex()
	// if lastLogIndex < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	reply.Success = false
	// 	return
	// }

	// Update reply.Success atomically
	reply.Success = true

	// Update currentTerm, reset election timer, and convert to Follower if needed
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo(Follower)
		rf.electionTimer.Reset(time.Duration(300+rand.Int31n(150)) * time.Millisecond)
	}

	// Append new entries not already in log
	// i := args.PrevLogIndex + 1
	// j := 0

	// for i < lastLogIndex+1 && j < len(args.Entries) {
	// 	if rf.log[i].Term != args.Entries[j].Term {
	// 		break
	// 	}
	// 	i++
	// 	j++
	// }

	// // Update logs
	// rf.log = append(rf.log[:i], args.Entries[j:]...)

	// // Update commitIndex
	// if args.CommitIndex > rf.commitIndex {
	// 	lastIndex := rf.GetLastLogIndex()
	// 	if args.CommitIndex < lastIndex {
	// 		rf.commitIndex = args.CommitIndex
	// 	} else {
	// 		rf.commitIndex = lastIndex
	// 	}
	// }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (4B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	}

	rf.log = append(rf.log, &LogEntry{
		Term:    term,
		Command: command,
	})

	index := rf.GetLastLogIndex()

	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.electionTimer.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		select {
		case <-rf.electionTimer.C:
			DPrintf("Starting election")
			rf.mu.Lock()
			rf.electionTimer.Reset(time.Duration(300+rand.Int31n(150)) * time.Millisecond)

			if rf.state == Leader {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			rf.startElection()
		case <-rf.heartbeatTicker.C:
			DPrintf("Broadcasting heartbeat")
			rf.broadcastHeartbeat()
		}
	}
}

func (rf *Raft) startElection() {
	rf.convertTo(Candidate)

	rf.mu.Lock()
	totalReceivedResponse := 1
	voteCount := 1
	res := make(chan bool)
	last_term := 0

	if len(rf.log) > 0 {
		last_term = rf.log[len(rf.log)-1].Term
	}

	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  last_term,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(id int) {
			voteReply := RequestVoteReply{}
			rf.sendRequestVote(id, &voteArgs, &voteReply)
			if voteReply.VoteGranted {
				res <- true
			} else {
				res <- false
			}
		}(i)
	}

	// Count votes to see if current raft will become president
	for {
		voteResult := <-res
		totalReceivedResponse += 1
		if voteResult {
			voteCount += 1
		}
		if voteCount > len(rf.peers)/2 || totalReceivedResponse >= len(rf.peers) {
			break
		}
	}

	// Check if it's still a candidate
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	if voteCount > len(rf.peers)/2 {
		rf.convertTo(Leader)
		rf.broadcastHeartbeat()
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		Entries:  rf.log,
	}
	rf.mu.Unlock()

	// Broadcast heartbeat to all peers
	for i := 0; i < len(rf.peers); i++ {
		reply := AppendEntriesReply{}

		go func(id int) {
			if id == rf.me {
				return
			}
			rf.sendAppendEntries(id, &args, &reply)
		}(i)
	}
}

func (rf *Raft) convertTo(state int) {
	switch state {
	case Follower:
		rf.state = Follower
		rf.votedFor = -1
	case Candidate:
		rf.state = Candidate
		rf.currentTerm++
		rf.votedFor = rf.me
	case Leader:
		rf.state = Leader
		rf.electionTimer.Reset(time.Duration(300+rand.Int31n(150)) * time.Millisecond)
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (4A, 4B).
	rf.currentTerm = 0
	rf.convertTo(Follower)

	rf.electionTimer = time.NewTimer(time.Duration(300+rand.Int31n(150)) * time.Millisecond)
	rf.heartbeatTicker = time.NewTicker(100 * time.Millisecond)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = append(rf.log, &LogEntry{Term: 0})

	rf.applyCh = applyCh

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
