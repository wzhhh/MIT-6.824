package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
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

	minElecTime      = 300 * time.Millisecond
	maxElecTime      = 600 * time.Millisecond
	heartbeatTime    = 150 * time.Millisecond
	heartbeatTimeOut = 150 * time.Millisecond

	replicateInterval = 100 * time.Millisecond
	replicateTimeout  = 200 * time.Millisecond
	replyMsgInterval  = 150 * time.Millisecond
	retryInterval     = 20 * time.Millisecond

	_ = iota
	LogInConsistency
	StaleTerm
)

//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm int
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

	CurrentTerm int
	VotedFor    int // -1 if new, leader id
	voteMap     map[int]int

	Log []LogEntry // command and term

	commitIndex int // index of the highest Log entry know to be committed, first is 0
	lastApplied int // index of the highest Log entry applied, new is 0

	// reinitialized after election
	nextIndex  []int // leader: for each server, index of the next Log entry to send to that server, first is leader's last+1
	matchIndex []int // leader: highest Log entry known to be replicated on each server, new is 0

	lastTime        time.Time
	electionTimeout int
}

type LogEntry struct {
	Msg  ApplyMsg
	Term int
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
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
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.CurrentTerm)
	_ = e.Encode(rf.VotedFor)
	_ = e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var rfLog []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&rfLog) != nil {
		_, _ = DPrintf("[S%d, T%d] readPersist decode fail", rf.me, rf.CurrentTerm)
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = currentTerm
		rf.VotedFor = voteFor
		rf.Log = rfLog
		rf.mu.Unlock()
	}
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

	_, _ = DPrintf("[S%d T%d] RequestVote from S%d, T%d, prev vote S%d", rf.me, rf.CurrentTerm, args.CandidateId, args.Term, rf.VotedFor)
	reply.Term = rf.CurrentTerm
	rf.lastTime = time.Now()
	if args.Term < rf.CurrentTerm {
		_, _ = DPrintf("[S%d T%d] refuse RequestVote vote for S%d T%d", rf.me, rf.CurrentTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
		return
	}
	if rf.CurrentTerm < args.Term {
		rf.state = Follower
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}
	//rf.CurrentTerm = args.Term
	lastLog := rf.Log[len(rf.Log)-1]
	upToDate := false
	if lastLog.Term != args.LastLogTerm {
		upToDate = args.LastLogTerm > lastLog.Term
	} else {
		upToDate = args.LastLogIndex >= lastLog.Msg.CommandIndex
	}
	if (rf.VotedFor < 0 || rf.VotedFor == args.CandidateId) && upToDate {

		if rf.voteMap[rf.CurrentTerm] == 0 {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.lastTime = time.Now()
			_, _ = DPrintf("[S%d T%d] RequestVote vote for %d", rf.me, rf.CurrentTerm, args.CandidateId)
			rf.voteMap[rf.CurrentTerm]++
			rf.persist()
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
	Term        int
	Success     bool
	FailReason  int8
	ConflictIndex int
	ConflictTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	rf.lastTime = time.Now()
	reply.Success = true
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	if len(args.Entries) > 0 {
		_, _ = DPrintf("[S%d T%d] AppendEntries args, curLogLen: %d, prevLog: %d %d, leaderCommit: %d, entries: %v",
			rf.me, rf.CurrentTerm, len(rf.Log),
			args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
	}
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.FailReason = StaleTerm
		_, _ = DPrintf("[S%d T%d] refuse AppendEntries from S%d T%d", rf.me, rf.CurrentTerm, args.LeaderId, args.Term)

		return
	}

	if rf.state == Candidate || args.Term > rf.CurrentTerm {
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
		}
		rf.VotedFor = -1
		rf.state = Follower
		rf.persist()
	}

	//if len(rf.Log) < args.PrevLogIndex+1 {
	//	reply.Success = false
	//	reply.FailReason = LogInConsistency
	//	reply.CommitIndex = len(rf.Log)-1
	//	_, _ = DPrintf("[S%d T%d] AppendEntries no exist Log entry at prevLogIndex: %d", rf.me, rf.CurrentTerm, args.PrevLogIndex)
	//	return
	//}
	if len(rf.Log)-1 < args.PrevLogIndex || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.FailReason = LogInConsistency

		if len(rf.Log)-1 < args.PrevLogIndex {
			reply.ConflictIndex = len(rf.Log) - 1
			return
		}

		reply.ConflictIndex = args.PrevLogIndex
		reply.ConflictTerm = rf.Log[reply.ConflictIndex].Term
		for reply.ConflictIndex > rf.commitIndex && rf.Log[reply.ConflictIndex].Term == reply.ConflictTerm {
			reply.ConflictIndex--
		}
		_, _ = DPrintf("[S%d T%d] AppendEntries no match Log entry at prevLogIndex: %d, prevLogTerm: %d, replyIndex: %d)",
			rf.me, rf.CurrentTerm, args.PrevLogIndex, args.PrevLogTerm, reply.ConflictIndex)

		return
	}
	//if len(args.Entries) > 0 {
	//	// TODO
	//	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
	//		if rf.state != Leader {
	//			rf.Log = rf.Log[:args.PrevLogIndex+1]
	//		}
	//		rf.Log = append(rf.Log, args.Entries...)
	//	} else {
	//		entryIndex, entryTerm, i := 0, 0, 0
	//		for i < len(args.Entries) {
	//			entryIndex = args.Entries[i].Msg.CommandIndex
	//			entryTerm = args.Entries[i].Term
	//			if entryIndex >= len(rf.Log) {
	//				break
	//			}
	//			if rf.Log[entryIndex].Term != entryTerm {
	//				break
	//			}
	//			i++
	//		}
	//		if entryIndex < len(rf.Log) {
	//			if rf.state != Leader {
	//				rf.Log = rf.Log[:entryIndex]
	//			}
	//		}
	//		rf.Log = append(rf.Log, args.Entries[i:]...)
	//	}
	//
	//}

	if len(args.Entries) > 0 {
		if rf.state != Leader {
			rf.Log = rf.Log[:args.PrevLogIndex+1]
		}
		rf.Log = append(rf.Log, args.Entries...)
		rf.persist()
	}

	//rf.lastApplied = len(rf.Log) - 1
	if len(args.Entries) > 0 {
		_, _ = DPrintf("[S%d T%d] AppendEntries new entry append, cur Log: %v", rf.me, rf.CurrentTerm, rf.Log)
	}

	if args.LeaderCommit > rf.commitIndex {
		old := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.Log)-1)
		_, _ = DPrintf("[S%d T%d] AppendEntries update commitIndex, old: %d, new: %d", rf.me, rf.CurrentTerm, old, rf.commitIndex)
	}

}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
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
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
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

	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	_, _ = DPrintf("[S%d T%d] T%d Start receive command %v", rf.me, term, rf.CurrentTerm, command)
	//rf.lastApplied++
	index = len(rf.Log)
	rf.Log = append(rf.Log, LogEntry{
		Msg: ApplyMsg{
			Command:      command,
			CommandIndex: index,
			CommandTerm: term,
		},
		Term: term,
	})
	rf.persist()
	_, _ = DPrintf("[S%d T%d] Start receive command done %v, Log: %v", rf.me, term, command, rf.Log)

	rf.mu.Unlock()
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
	rf.CurrentTerm++
	rf.state = Candidate
	rf.lastTime = time.Now()
	term := rf.CurrentTerm
	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := rf.Log[len(rf.Log)-1].Term
	rf.persist()
	//rf.VotedFor = rf.me
	rf.mu.Unlock()

	ok := rf.election(term, lastLogIndex, lastLogTerm, t)
	if !ok {
		_, _ = DPrintf("[S%d T%d] election fail", rf.me, term)
		rf.mu.Lock()
		rf.VotedFor = -1
		rf.state = Follower
		rf.persist()
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	if rf.state == Follower {
		rf.VotedFor = -1
		rf.persist()
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.convertToLeader()
}

func (rf *Raft) election(term, lastLogIndex, lastLogTerm int, t time.Duration) bool {
	var wg sync.WaitGroup
	voted := 1
	voteChan := make(chan *RequestVoteReply, len(rf.peers))
	done := make(chan struct{})
	defer close(done)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.mu.Lock()
			if rf.voteMap[rf.CurrentTerm] == 0 {
				rf.VotedFor = rf.me
				rf.persist()
				rf.lastTime = time.Now()
				_, _ = DPrintf("[S%d T%d] election vote for self", rf.me, rf.CurrentTerm)
				rf.voteMap[rf.CurrentTerm]++
			}
			rf.mu.Unlock()
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			voteArgs := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			voteReply := &RequestVoteReply{}
			_, _ = DPrintf("[S%d, T%d -> S%d] send request vote\n", rf.me, term, i)
			ok := rf.sendRequestVote(i, voteArgs, voteReply)
			if !ok {
				return
			}
			rf.mu.Lock()
			if rf.state != Candidate || rf.CurrentTerm != voteArgs.Term {
				//log.Printf("vote term not equal 0")
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
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
		defer close(resChan)
		var voteSucceed = func() bool {
			for voteReply := range voteChan {
				if voteReply.Term > term {
					rf.mu.Lock()
					rf.CurrentTerm = voteReply.Term
					rf.state = Follower
					rf.VotedFor = -1
					rf.persist()
					_, _ = DPrintf("[S%d T%d] vote response term bigger than now, update to T%d", rf.me, term, voteReply.Term)
					rf.mu.Unlock()
					return false
				}

				if voteReply.VoteGranted {
					voted++
				}

				if voted > len(rf.peers)/2 {
					return true
				}
			}
			return false
		}
		select {
		case <-done:
			return
		case resChan <- voteSucceed():
			return
		}
	}()

	select {
	case <-time.After(t):
		_, _ = DPrintf("[S%d T%d] election timeout", rf.me, term)
		return false
	case res := <-resChan:
		return res
	}
}

func (rf *Raft) convertToLeader() {
	rf.initializeOnLeader()
	go rf.checkAndAppendEntries()
}

func (rf *Raft) initializeOnLeader() {
	rf.mu.Lock()
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.Log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	_, _ = DPrintf("[S%d T%d] become leader, send heartbeat, nextIndex: %v, Log: %v \n", rf.me, rf.CurrentTerm, rf.nextIndex, rf.Log)
	rf.mu.Unlock()
}

func (rf *Raft) checkAndAppendEntries() {
	rf.mu.Lock()
	currentTerm := rf.CurrentTerm
	rf.mu.Unlock()
	term, isLeader := currentTerm, true
	count := 0
	for {
		if rf.killed() {
			rf.reset("checkAndAppendEntries")
			return
		}
		rf.mu.Lock()
		rf.lastTime = time.Now()
		leaderCommit := rf.commitIndex
		term = rf.CurrentTerm
		isLeader = rf.state == Leader
		if !isLeader || term != currentTerm {
			_, _ = DPrintf("[S%d T%d] exit heartbeat due to not leader or not equal to T%d\n", rf.me, term, currentTerm)
			rf.mu.Unlock()
			return
		}

		entries := make([][]LogEntry, len(rf.peers))
		prevLogIndex := make([]int, len(rf.peers))
		prevLogTerm := make([]int, len(rf.peers))
		term = rf.CurrentTerm
		lastLogIndex := len(rf.Log) - 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			prevLogIndex[i] = rf.nextIndex[i] - 1
			prevLogTerm[i] = rf.Log[rf.nextIndex[i]-1].Term
			if lastLogIndex >= rf.nextIndex[i] {
				_, _ = DPrintf("[S%d T%d] replicateLog to S%d, last: %d, next: %d", rf.me, term, i, lastLogIndex, rf.nextIndex[i])
				entry := make([]LogEntry, len(rf.Log[rf.nextIndex[i]:]))
				copy(entry, rf.Log[rf.nextIndex[i]:])
				entries[i] = entry
			}
		}
		rf.mu.Unlock()

		ok, r := rf.broadcastAppendEntries(term, leaderCommit, lastLogIndex, count, entries, prevLogIndex, prevLogTerm)
		if !ok {
			_, _ = DPrintf("[S%d T%d] heartbeat exit due to higher term response", rf.me, term)
			return
		}
		if r == 0 {
			_, _ = DPrintf("[S%d T%d] exit heartbeat due to 0 response", rf.me, term)
			return
		}
		count++
		time.Sleep(heartbeatTime)
	}
}

// compare the current term with the term you sent in your original RPC. If the two are different, drop the reply and return
func (rf *Raft) broadcastAppendEntries(term, leaderCommit, lastLogIndex, count int, entries [][]LogEntry, prevLogIndex, prevLogTerm []int) (bool, int) {

	var wg sync.WaitGroup
	done := make(chan struct{})
	defer close(done)
	stopCh := make(chan struct{})
	r := int32(0)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int, entries []LogEntry) {
			defer wg.Done()

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex[i],
				PrevLogTerm:  prevLogTerm[i],
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			reply := &AppendEntriesReply{}

			if len(entries) > 0 {
				_, _ = DPrintf("[S%d T%d] send replicate Log to S%d, prevLogIndex: %d, prelogTerm: %d, entries: %v, "+
					"leaderCommit: %d", rf.me, term, i, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
				rf.replicateLogToOne(done, i, args, reply, 0, count, &r)
				rf.mu.Lock()
				if rf.state != Leader || rf.CurrentTerm != args.Term {
					//log.Printf("term not equal 1")
					atomic.StoreInt32(&r, 1)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				if reply.Success {
					rf.mu.Lock()
					if rf.nextIndex[i] > rf.matchIndex[i] {
						rf.nextIndex[i] = lastLogIndex + 1
						rf.matchIndex[i] = lastLogIndex
						_, _ = DPrintf("[S%d T%d] replicateLog update follower S%d, next: %d, match: %d", rf.me, term, i, rf.nextIndex[i], rf.matchIndex[i])

					}
					rf.mu.Unlock()
					atomic.StoreInt32(&r, 1)
					return
				}
			} else {
				ok := rf.sendAppendEntries(i, args, reply)
				if !ok {
					select {
					case <-done:
						return
					default:
						_, _ = DPrintf("[S%d T%d] heartbeat lost to S%d", rf.me, term, i)
					}

					//atomic.StoreInt32(&r, 1)
					return
				}
				rf.mu.Lock()
				if rf.state != Leader || rf.CurrentTerm != args.Term {
					//log.Printf("term not equal 2")
					atomic.StoreInt32(&r, 1)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}


			if reply.Term > term {
				rf.mu.Lock()
				rf.CurrentTerm = reply.Term
				rf.state = Follower
				rf.VotedFor = -1
				rf.persist()
				rf.mu.Unlock()
				select {
				case <-done:
					return
				case stopCh <- struct{}{}:
					_, _ = DPrintf("[S%d T%d] replicateLog response term bigger than now, become follower, term update to T%d, early stop", rf.me, term, reply.Term)

				}
				atomic.StoreInt32(&r, 1)
				return
			}
			atomic.StoreInt32(&r, 1)
		}(i, entries[i])
	}
	go func() {
		wg.Wait()
		close(stopCh)
	}()

	select {
	case <-time.After(heartbeatTimeOut):
		_, _ = DPrintf("[S%d T%d] replicateLog timeout %d", rf.me, term, count)
		return true, int(atomic.LoadInt32(&r))
	case _, ok := <-stopCh:
		if ok {
			_, _ = DPrintf("[S%d T%d] replicateLog early stop %d", rf.me, term, count)
			return false, int(atomic.LoadInt32(&r))
		}
		_, _ = DPrintf("[S%d T%d] replicateLog done, count: %d", rf.me, term, count)
		return true, int(atomic.LoadInt32(&r))
	}
}

/*
quickly log backtrack process:
If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
If a follower does have prevLogIndex in its log, but the term does not match, it should return conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has term equal to conflictTerm.
Upon receiving a conflict response, the leader should first search its log for conflictTerm. If it finds an entry in its log with that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
If it does not find an entry with that term, it should set nextIndex = conflictIndex.
 */
func (rf *Raft) replicateLogToOne(done <-chan struct{}, i int, args *AppendEntriesArgs, reply *AppendEntriesReply, retry, count int, r *int32) {
	select {
	case <-done:
		_, _ = DPrintf("[S%d T%d] replicate Log to S%d early exit, retry: %d, count: %d", rf.me, args.Term, i, retry, count)
		return
	default:
	}
	ok := rf.sendAppendEntries(i, args, reply)
	if !ok {
		time.Sleep(retryInterval)
		_, _ = DPrintf("[S%d T%d] %d replicate Log fail on S%d, retry, count: %d", rf.me, args.Term, retry, i, count)
		rf.replicateLogToOne(done, i, args, reply, retry+1, count, r)
		return
	}
	atomic.StoreInt32(r, 1)
	rf.mu.Lock()
	if rf.state != Leader || rf.CurrentTerm != args.Term {
		//log.Printf("term not equal 3")
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	if !reply.Success && reply.FailReason == LogInConsistency {
		rf.mu.Lock()
		if reply.ConflictTerm == -1 {
			rf.nextIndex[i] = reply.ConflictIndex + 1
		} else {
			conflictIndex := -1
			for index := args.PrevLogIndex; index >= 1; index-- {
				if rf.Log[index].Term == reply.ConflictTerm {
					conflictIndex = index
					break
				}
			}
			if conflictIndex != -1 {
				rf.nextIndex[i] = conflictIndex+1
			} else {
				rf.nextIndex[i] = reply.ConflictIndex+1
			}
		}

		//if args.PrevLogIndex-rf.nextIndex[i] > 0 {
		//	Log.Printf("[S%d T%d] %d retry more than one on S%d, prev: %d, next: %d, count: %d", rf.me, args.Term,
		//		retry, i, args.PrevLogIndex, rf.nextIndex[i], count)
		//}
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
		entry := make([]LogEntry, len(rf.Log[rf.nextIndex[i]:]))
		copy(entry, rf.Log[rf.nextIndex[i]:])
		args.Entries = entry

		rf.mu.Unlock()
		_, _ = DPrintf("[S%d T%d] %d replicate Log inconsistency on S%d, retry, count: %d", rf.me, args.Term, retry, i, count)
		time.Sleep(retryInterval)
		rf.replicateLogToOne(done, i, args, reply, retry+1, count, r)
		return
	}
	return
}

func (rf *Raft) reset(loc string) {
	rf.mu.Lock()
	term := rf.CurrentTerm
	rf.state = Follower
	//rf.CurrentTerm = 0
	//rf.VotedFor = -1
	rf.voteMap = map[int]int{}
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.mu.Unlock()
	_, _ = DPrintf("[S%d T%d] %s exit due to killed\n", rf.me, term, loc)
}

func (rf *Raft) triggerApplyMsg(applyCh chan ApplyMsg) {
	for {
		if rf.killed() {
			rf.reset("applyCh")
			return
		}

		applyMsgs := make([]ApplyMsg, 0)

		rf.mu.Lock()
		isLeader := rf.state == Leader
		term := rf.CurrentTerm

		if isLeader {
			newCommitIndex := rf.commitIndex
			for n := rf.commitIndex + 1; n < len(rf.Log); n++ {
				if rf.Log[n].Term != rf.CurrentTerm {
					continue
				}
				count := 1
				for i, matchIndex := range rf.matchIndex {
					if i == rf.me {
						continue
					}
					if matchIndex >= n {
						count++
					}
					if count > len(rf.peers)/2 {
						newCommitIndex = n
						break
					}
				}
			}
			if newCommitIndex > rf.commitIndex {
				_, _ = DPrintf("[S%d T%d] leader commitIndex update %d -> %d", rf.me, term, rf.commitIndex, newCommitIndex)
				rf.commitIndex = newCommitIndex
			}
		}

		//committed := false
		for rf.lastApplied < rf.commitIndex {
			_, _ = DPrintf("[S%d T%d] apply Log, lastApplied: %d, commitIndex: %d", rf.me, term, rf.lastApplied, rf.commitIndex)

			rf.lastApplied++
			//rf.Log[rf.lastApplied].Msg.CommandValid = true
			//committed = true
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[rf.lastApplied].Msg.Command,
				CommandIndex: rf.Log[rf.lastApplied].Msg.CommandIndex,
				CommandTerm: rf.Log[rf.lastApplied].Msg.CommandTerm,
			}
			applyMsgs = append(applyMsgs, msg)

			//select {
			//case <-time.After(10 * time.Millisecond):
			//	_, _ = DPrintf("[S%d T%d] send msg back to applyCh timeout, %v", rf.me, term, rf.Log[rf.lastApplied].Msg)
			//	break
			//case applyCh <- msg:
			//	_, _ = DPrintf("[S%d T%d] send msg back to applyCh, %v", rf.me, term, msg)
			//	//log.Printf("[S%d %d] commit msg: %v", rf.me, term, msg)
			//}
		}
		//if committed {
		//	rf.persist()
		//}
		rf.mu.Unlock()
		for _, msg := range applyMsgs {
			_, _ = DPrintf("[S%d T%d] send msg back to applyCh, %v", rf.me, term, msg)
			//log.Printf("[S%d %d] commit msg: %#v", rf.me, term, msg)
			applyCh <- msg
		}

		//_, _ = DPrintf("[S%d T%d] triggerApplyMsg sleep", rf.me, term)
		time.Sleep(replyMsgInterval)
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
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.state = Follower
	rf.lastTime = time.Now()
	rf.voteMap = map[int]int{}
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.Log = []LogEntry{{Term: rf.CurrentTerm, Msg: ApplyMsg{
		CommandValid: false,
		Command:      nil,
		CommandIndex: rf.lastApplied,
	}}}
	go func() {
		t := time.Duration(makeSeed())%(maxElecTime-minElecTime) + minElecTime
		var cur time.Duration
		for {
			if rf.killed() {
				rf.reset("timeout")
				return
			}

			rf.mu.Lock()
			lastTime := rf.lastTime
			term := rf.CurrentTerm
			//lastApplied := rf.lastApplied
			rf.mu.Unlock()

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

	go rf.triggerApplyMsg(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
