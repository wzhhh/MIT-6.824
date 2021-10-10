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
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

// import "bytes"
// import "../labgob"

type ServerState int8

const (
	_ ServerState = iota
	Follower
	Candidate
	Leader

	minElecTime   = 300 * time.Millisecond
	maxElecTime   = 600 * time.Millisecond
	heartbeatTime = 100 * time.Millisecond
	polltime      = 200 * time.Millisecond
	electiontime  = 500 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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

	state ServerState

	currentTerm int
	votedFor    int // me
	voteMap map[int]int

	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastTime        time.Time
	electionTimeout int
}

type LogEntry struct {
	Msg  ApplyMsg
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
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
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	_, _ = DPrintf("[S%d T%d] RequestVote from S%d, T%d, now vote S%d", rf.me, rf.currentTerm, args.CandidateId, args.Term, rf.votedFor)
	reply.Term = rf.currentTerm
	rf.lastTime = time.Now()
	if args.Term < rf.currentTerm {
		_, _ = DPrintf("[S%d T%d] refuse RequestVote vote for S%d T%d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	//rf.currentTerm = args.Term
	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && args.LastLogTerm >= rf.log[rf.lastApplied].Term {
		if rf.voteMap[rf.currentTerm] == 0 {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			//rf.lastTime = time.Now()
			_, _ = DPrintf("[S%d T%d] RequestVote vote for %d", rf.me, rf.currentTerm, args.CandidateId)
			rf.voteMap[rf.currentTerm]++

		}
		return
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
	reply.Term = rf.currentTerm
	rf.lastTime = time.Now()
	if args.Term < rf.currentTerm {
		reply.Success = false
		_, _ = DPrintf("[S%d T%d] refuse AppendEntries from S%d T%d", rf.me, rf.currentTerm, args.LeaderId, args.Term)

		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if len(args.Entries) == 0 {
		reply.Success = true
		rf.state = Follower
		//rf.lastTime = time.Now()
		_, _ = DPrintf("[S%d T%d] Follower get AppendEntries from S%d T%d, become follower", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		//rf.currentTerm = args.Term
		//rf.lastTime = time.Now()
		return
	}
	_, _ = DPrintf("[S%d T%d] AppendEntries not match", rf.me, rf.currentTerm)
	//if len(rf.log) < args.PrevLogIndex+1 {
	//	reply.Success = false
	//	return
	//}
	//if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	//	reply.Success = false
	//	return
	//}

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
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) convertToCandidate(t time.Duration) {
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	rf.lastTime = time.Now()
	term := rf.currentTerm
	//rf.votedFor = rf.me
	rf.mu.Unlock()

	ok := rf.election(term, t)
	if !ok {
		_, _ = DPrintf("[S%d T%d] election fail", rf.me, term)
		rf.mu.Lock()
		rf.votedFor = -1
		rf.state = Follower
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	if rf.state == Follower {
		rf.votedFor = -1
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.convertToLeader(heartbeatTime)
}

func (rf *Raft) election(term int, t time.Duration) bool {
	var wg sync.WaitGroup
	voted := 1
	voteChan := make(chan *RequestVoteReply, len(rf.peers)-1)
	done := make(chan struct{})
	defer close(done)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			voteArgs := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastApplied,
				LastLogTerm:  rf.log[rf.lastApplied].Term,
			}
			voteReply := &RequestVoteReply{}
			_, _ = DPrintf("[S%d, T%d -> S%d] send request vote\n", rf.me, term, i)
			ok := rf.sendRequestVote(i, voteArgs, voteReply)
			if !ok {
				return
			}
			select {
			case <-done:
				return
			case voteChan <- voteReply:
			}

		}(i)
	}
	go func() {
		wg.Wait()
		close(voteChan)
	}()
	resChan := make(chan bool)
	go func() {
		maxTerm := term
		for voteReply := range voteChan {
			if voteReply.Term > maxTerm {
				maxTerm = voteReply.Term
			}

			if !voteReply.VoteGranted {
				continue
			}
			voted++
			if voted > len(rf.peers)/2 {
				if maxTerm > term {
					rf.mu.Lock()
					rf.currentTerm = maxTerm
					rf.votedFor = -1
					rf.state = Follower
					_, _ = DPrintf("[S%d T%d] vote response term bigger than now, update to T%d", rf.me, term, maxTerm)
					rf.mu.Unlock()
					//resChan <- false
					//return
				}
				resChan <- true
			}
		}
		if maxTerm > term {
			rf.mu.Lock()
			rf.currentTerm = maxTerm
			rf.votedFor = -1
			_, _ = DPrintf("[S%d T%d] vote response term bigger than now, update to T%d", rf.me, term, maxTerm)
			rf.state = Follower
			rf.mu.Unlock()
		}
		resChan <- false


	}()

	for {
		select {
		case <-time.After(t):
			_, _ = DPrintf("[S%d T%d] election timeout", rf.me, term)
			return false
		case res := <-resChan:
			return res
		}
	}
}

func (rf *Raft) convertToLeader(timeout time.Duration) {
	rf.mu.Lock()
	rf.state = Leader
	_, _ = DPrintf("[S%d T%d] become leader, send heartbeat\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	go func() {
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		term, isLeader := currentTerm, true
		for {
			rf.mu.Lock()
			rf.lastTime = time.Now()
			rf.mu.Unlock()
			term, isLeader = rf.GetState()
			if !isLeader || term != currentTerm {
				_, _ = DPrintf("[S%d T%d] exit heartbeat due to not leader or not equal to T%d\n", rf.me, term, currentTerm)
				return
			}
			if rf.killed() {
				rf.mu.Lock()
				rf.state = Follower
				rf.currentTerm = 0
				rf.votedFor = -1
				rf.voteMap = map[int]int{}
				rf.mu.Unlock()
				_, _ = DPrintf("[S%d T%d] exit heartbeat due to killed\n", rf.me, term)
				return
			}
			ok, r := rf.heartBeat(term, timeout)
			if !ok {
				_, _ = DPrintf("[S%d T%d] heartbeat exit due to higher term response", rf.me, term)
				return
			}
			if r == 0 {
				_, _ = DPrintf("[S%d T%d] exit heartbeat due to 0 response", rf.me, term)
				return
			}
			time.Sleep(heartbeatTime)
		}
	}()
}

func (rf *Raft) heartBeat(term int, timeout time.Duration) (bool, int) {
	var wg sync.WaitGroup
	stopChan := make(chan struct{})
	done := make(chan struct{})
	defer close(done)
	//var mu sync.Mutex
	r := int32(0)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int, term int) {
			defer wg.Done()
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: 0,
			}
			reply := &AppendEntriesReply{}

			ok := rf.sendAppendEntries(i, args, reply)
			if !ok {
				select {
				case <-done:
					return
				default:
					_, _ = DPrintf("[S%d T%d] heartbeat fail on S%d", rf.me, term, i)
				}

				atomic.StoreInt32(&r, 1)
				return
			}
			if reply.Term > term {
				rf.mu.Lock()
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				//rf.lastTime = time.Now()
				rf.mu.Unlock()
				select {
				case <-done:
					return
				case stopChan <- struct{}{}:
					_, _ = DPrintf("[S%d T%d] heartbeat response term bigger than now, update to T%d", rf.me, term, reply.Term)
				}
				atomic.StoreInt32(&r, 1)
				return
			}
			atomic.StoreInt32(&r, 1)
		}(i, term)
	}
	go func() {
		wg.Wait()
		close(stopChan)
	}()
	for {
		select {
		case <-time.After(timeout):
			_, _ = DPrintf("[S%d T%d] heartbeat timeout", rf.me, term)
			return true, int(atomic.LoadInt32(&r))
		case _, ok := <-stopChan:
			if ok {
				return false, int(atomic.LoadInt32(&r))
			}
			return true, int(atomic.LoadInt32(&r))
		}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.state = Follower
	rf.lastTime = time.Now()
	rf.voteMap = map[int]int{}
	rf.log = []LogEntry{{Term: rf.currentTerm}}
	go func() {
		t := time.Duration(makeSeed())%(maxElecTime-minElecTime) + minElecTime
		var cur time.Duration
		for {
			rf.mu.Lock()
			lastTime := rf.lastTime
			term := rf.currentTerm
			rf.mu.Unlock()
			if rf.killed() {
				rf.mu.Lock()
				rf.state = Follower
				rf.currentTerm = 0
				rf.votedFor = -1
				rf.voteMap = map[int]int{}
				rf.mu.Unlock()
				_, _ = DPrintf("[S%d T%d] exit due to killed\n", rf.me, term)
				return
			}
			if time.Since(lastTime) >= t {
				_, _ = DPrintf("[S%d T%d] timeout become candidate %v\n", rf.me, term, t)
				rf.convertToCandidate(t)
			}

			time.Sleep(t)
			cur = t
			t = time.Duration(makeSeed())%(maxElecTime-minElecTime) + minElecTime
			_ = cur
			//_, _ = DPrintf("[S%d T%d] daemon ok in interval %v, next %v", rf.me, term, cur, t)
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
