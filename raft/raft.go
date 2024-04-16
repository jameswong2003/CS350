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
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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

type LogEntry struct {
	Term    int
	Command interface{}
}

// Different types of states
const (
	Leader = iota
	Candidate
	Follower
)

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

	state       int
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	numVotes           int
	applyCh            chan ApplyMsg
	electionWinChan    chan bool
	becomeFollowerChan chan bool
	grantVoteChan      chan bool
	heartbeatChan      chan bool
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
	buffer := new(bytes.Buffer)
	encode := gob.NewEncoder(buffer)
	encode.Encode(rf.currentTerm)
	encode.Encode(rf.votedFor)
	encode.Encode(rf.log)
	rf.persister.SaveRaftState(buffer.Bytes())
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
	buffer := bytes.NewBuffer(data)
	decode := gob.NewDecoder(buffer)
	decode.Decode(&rf.currentTerm)
	decode.Decode(&rf.votedFor)
	decode.Decode(&rf.log)
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.persist()
		return
	}

	if rf.currentTerm < args.Term {
		rf.convertTo(Follower)
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	upToDate := false
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term

	if args.LastLogTerm == lastLogTerm {
		if args.LastLogIndex >= lastLogIndex {
			upToDate = true
		}
	} else {
		if args.LastLogTerm > lastLogTerm {
			upToDate = true
		}
	}

	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.notifyChannel(rf.grantVoteChan)
	}
	rf.persist()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		return
	}

	if rf.currentTerm < args.Term {
		rf.convertTo(Follower)
		rf.currentTerm = args.Term
	}

	lastIndex := len(rf.log) - 1
	rf.notifyChannel(rf.heartbeatChan)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > lastIndex { // follower log is shorter than leader log
		reply.ConflictIndex = lastIndex + 1
		return
	}

	rfPrevLogTerm := rf.log[args.PrevLogIndex].Term
	if rfPrevLogTerm != args.PrevLogTerm {
		// loop through to check if log contains an entry at prevlogindex whose term matches prevlogterm
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term == rfPrevLogTerm {
				reply.ConflictIndex = i
			}
		}
		reply.Success = false
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	i := args.PrevLogIndex + 1
	j := 0

	for i < lastIndex+1 && j < len(args.Entries) {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
		i++
		j++
	}

	rf.log = append(rf.log[:i], args.Entries[j:]...)

	// Successfully updated
	reply.Success = true

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
		go rf.commitlog()
	}

	rf.persist()
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

	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate || reply.Term < rf.currentTerm || rf.currentTerm != args.Term {
		return ok
	}

	if rf.currentTerm < reply.Term {
		rf.convertTo(Follower)
		rf.currentTerm = reply.Term
		rf.persist()
		return ok
	}

	if reply.VoteGranted {
		rf.numVotes++
		// only send once when vote count just reaches majority
		if rf.numVotes > len(rf.peers)/2 {
			rf.notifyChannel(rf.electionWinChan)
		}
	}

	return ok
}

// Send out heartbeats to peers
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.currentTerm != args.Term || reply.Term < rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.convertTo(Follower)
		rf.currentTerm = reply.Term
		rf.persist()
		return ok
	}

	// Update matchIndex and nextIndex of the follower based on the reply
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		// Handle the case where the AppendEntries RPC was not successful
		if reply.ConflictTerm == -1 {
			// Follower's log is shorter than the leader's log
			rf.nextIndex[server] = reply.ConflictIndex
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		} else {
			// Try to find the conflictTerm in the follower's log
			newNextIndex := len(rf.log) - 1
			for ; newNextIndex >= 0; newNextIndex-- {
				if rf.log[newNextIndex].Term == reply.ConflictTerm {
					break
				}
			}
			if newNextIndex > 0 {
				rf.nextIndex[server] = newNextIndex
			} else {
				// If the conflictTerm is not found, set nextIndex to conflictIndex
				rf.nextIndex[server] = reply.ConflictIndex
			}
			// Update matchIndex accordingly
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	}

	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
	for N := len(rf.log) - 1; N >= rf.commitIndex; N-- {
		nCount := 1
		if rf.log[N].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N {
					nCount += 1
				}
			}
		}

		// Check for majority
		if nCount > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.commitlog()
			break
		}
	}
	rf.persist()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		term = rf.currentTerm
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		rf.persist()
	}

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
		// time.Sleep().
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		heartbeatTimer := 80 * time.Millisecond
		electionTimer := time.Duration(rand.Intn(150)+300) * time.Millisecond

		switch state {
		case Leader:
			select {
			case <-rf.becomeFollowerChan:
			case <-time.After(heartbeatTimer):
				rf.mu.Lock()
				rf.broadcastHeartBeat()
				rf.mu.Unlock()
			}
		case Follower:
			select {
			case <-rf.grantVoteChan:
			case <-rf.heartbeatChan:
			case <-time.After(electionTimer):
				rf.convertTo(Candidate)
			}
		case Candidate:
			select {
			case <-rf.becomeFollowerChan:
			case <-rf.electionWinChan:
				rf.convertTo(Leader)
			case <-time.After(electionTimer):
				rf.convertTo(Candidate)
			}
		}

	}
}

// Start election
func (rf *Raft) startElection() {
	// send request vote to all peers
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}
}

func (rf *Raft) broadcastHeartBeat() {
	if rf.state != Leader {
		return
	}

	// send appendEntries to all peers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
			Entries:      rf.log[rf.nextIndex[i]:],
		}

		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &reply)
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.numVotes = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.electionWinChan = make(chan bool)
	rf.becomeFollowerChan = make(chan bool)
	rf.grantVoteChan = make(chan bool)
	rf.heartbeatChan = make(chan bool)
	rf.log = append(rf.log, LogEntry{Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start the background server loop
	go rf.ticker()

	return rf
}

// Convert current raft to a different state
func (rf *Raft) convertTo(state int) {
	switch state {
	case Leader:
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.state = Leader
		// reinitialize volatile state after election
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		newNextIndex := len(rf.log)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = newNextIndex
		}

		rf.broadcastHeartBeat()
	case Candidate:
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.currentTerm += 1
		rf.state = Candidate
		rf.votedFor = rf.me
		rf.numVotes = 1
		rf.persist()

		rf.startElection()
	case Follower:
		rf.state = Follower
		rf.votedFor = -1
	}
}

// Apply new log
func (rf *Raft) commitlog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}
}

func (rf *Raft) notifyChannel(ch chan bool) {
	select {
	case ch <- true:
	default:
	}
}
