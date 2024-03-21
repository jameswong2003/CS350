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

const (
	Leader = iota
	Candidate
	Follower
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
	state int32

	currentTerm int
	votedFor    int

	electionTimerChan       chan bool
	heartbeatTimerChan      chan bool
	lastElectionResetTimer  int64
	lastHeartbeatResetTimer int64
	timeoutHeartbeat        int64
	timeoutElection         int64

	// Volatile state on all servers
	log         []LogEntry
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
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

	if args.Term < rf.currentTerm || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertTo(Follower)
		rf.currentTerm = args.Term
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm

		rf.resetElectionTimer()
	}
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.resetElectionTimer()

	if args.Term > rf.currentTerm {
		rf.convertTo(Follower)
		rf.currentTerm = args.Term
	} else if rf.state == Candidate {
		rf.state = Follower
	}

	reply.Term = rf.currentTerm
	reply.Success = true
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (4B).

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
		select {
		case <-rf.electionTimerChan:
			rf.startElection()
		case <-rf.heartbeatTimerChan:
			rf.broadcastHeartbeat()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.convertTo(Candidate)
	rf.currentTerm++
	vote := 1
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(candidateId int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: candidateId,
			}
			rf.mu.Unlock()

			reply := RequestVoteReply{}

			if rf.sendRequestVote(candidateId, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm != reply.Term {
					return
				}

				if reply.VoteGranted {
					vote++

					if vote > len(rf.peers)/2 && rf.state == Candidate {
						rf.convertTo(Leader)
						rf.mu.Unlock()
						rf.broadcastHeartbeat()
						rf.mu.Lock()
					}
				} else {
					if rf.currentTerm < reply.Term {
						rf.convertTo(Follower)
						rf.currentTerm = reply.Term
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	rf.lastHeartbeatResetTimer = time.Now().UnixNano()
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(candidateId int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(candidateId, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Leader {
					return
				}

				if rf.currentTerm != args.Term {
					return
				}

				if !reply.Success {
					if reply.Term > rf.currentTerm {
						rf.convertTo(Follower)
						rf.currentTerm = reply.Term
						return
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) electionTimer() {
	for {
		rf.mu.Lock()

		if rf.state != Leader {
			timeElapsed := (time.Now().UnixNano() - int64(rf.lastElectionResetTimer)) / time.Hour.Milliseconds()

			if timeElapsed > rf.timeoutElection {
				rf.electionTimerChan <- true
			}
		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) heartbeatTimer() {
	for {
		rf.mu.Lock()

		if rf.state == Leader {
			timeElapsed := (time.Now().UnixNano() - rf.lastHeartbeatResetTimer) / int64(time.Millisecond)

			if timeElapsed > rf.timeoutHeartbeat {
				rf.heartbeatTimerChan <- true
			}
		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 5)
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.timeoutElection = rf.timeoutHeartbeat*5 + rand.Int63n(150)
	rf.lastElectionResetTimer = time.Now().UnixNano()
}

func (rf *Raft) convertTo(state int) {
	switch state {
	case Leader:
		rf.state = Leader
		rf.lastHeartbeatResetTimer = time.Now().UnixNano()
		// fmt.Println(rf.me, " to leader")
	case Candidate:
		rf.state = Candidate
		rf.votedFor = rf.me
		rf.currentTerm++
		rf.resetElectionTimer()
		// fmt.Println(rf.me, " to candidate")
	case Follower:
		rf.state = Follower
		rf.votedFor = -1
		// fmt.Println(rf.me, " to follower")
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

	rf.heartbeatTimerChan = make(chan bool)
	rf.electionTimerChan = make(chan bool)
	rf.timeoutHeartbeat = 100
	rf.resetElectionTimer()

	// start ticker goroutine to start elections.
	go rf.ticker()
	go rf.electionTimer()
	go rf.heartbeatTimer()

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	return rf
}
