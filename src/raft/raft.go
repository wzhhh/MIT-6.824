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
	heartbeatTime    = 100 * time.Millisecond
	heartbeatTimeOut = 150 * time.Millisecond

	replicateInterval = 100 * time.Millisecond
	replicateTimeout  = 200 * time.Millisecond
	replyMsgInterval  = 100 * time.Millisecond
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
	CommandValid      bool
	Command           interface{}
	CommandIndex      int
	CommandTerm       int
	Snapshot          []byte
	LastIncludedIndex int
	LastIncludedTerm  int
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

	applyCh chan ApplyMsg
	state   ServerState

	CurrentTerm       int
	VotedFor          int        // -1 if new, leader id
	Log               []LogEntry // command and term
	LastIncludedIndex int
	LastIncludedTerm  int

	voteMap     map[int]int
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
	data := rf.raftStateForPersist()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) raftStateForPersist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.CurrentTerm)
	_ = e.Encode(rf.VotedFor)
	_ = e.Encode(rf.Log)
	_ = e.Encode(rf.LastIncludedIndex)
	_ = e.Encode(rf.LastIncludedTerm)
	return w.Bytes()
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
	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&rfLog) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		_, _ = DPrintf("[S%d, T%d] readPersist decode fail", rf.me, rf.CurrentTerm)
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = currentTerm
		rf.VotedFor = voteFor
		rf.Log = rfLog
		rf.LastIncludedIndex = lastIncludedIndex
		rf.LastIncludedTerm = lastIncludedTerm
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
	//rf.lastTime = time.Now()
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
	//lastLog := rf.Log[len(rf.Log)-1]
	upToDate := false
	lastLogTerm := rf.lastTerm()
	if lastLogTerm != args.LastLogTerm {
		upToDate = args.LastLogTerm > lastLogTerm
	} else {
		upToDate = args.LastLogIndex >= rf.lastIndex()
	}
	if (rf.VotedFor < 0 || rf.VotedFor == args.CandidateId) && upToDate {

		if rf.voteMap[rf.CurrentTerm] == 0 {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.lastTime = time.Now()
			_, _ = DPrintf("[S%d T%d] RequestVote vote for %d", rf.me, rf.CurrentTerm, args.CandidateId)
			rf.voteMap[rf.CurrentTerm]++
			rf.lastTime = time.Now()
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
	Term          int
	Success       bool
	FailReason    int8
	ConflictIndex int
	ConflictTerm  int
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

	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.ConflictIndex = 1
		reply.Success = false
		return
	}
	if args.PrevLogIndex == rf.LastIncludedIndex {
		if args.PrevLogTerm != rf.LastIncludedTerm {
			reply.ConflictIndex = 1
			reply.Success = false
			return
		}
		// 如果正好是snapshot最后一个且一致，说明log已经截断，不能再比较arg.PrevLogIndex在log的term（会是填充的log的term 0）， 故下面用else
		_, _ = DPrintf("[S%d T%d] AppendEntries args.PrevLogIndex[%d] == rf.LastIncludedIndex[%d] && args.PrevLogTerm[%d] == rf.LastIncludedTerm[%d]",
			rf.me, rf.CurrentTerm, args.PrevLogIndex, rf.LastIncludedIndex, args.PrevLogTerm, rf.LastIncludedTerm)
	} else {
		if rf.lastIndex() < args.PrevLogIndex || rf.Log[rf.index2LogPos(args.PrevLogIndex)].Term != args.PrevLogTerm {
			reply.Success = false
			if rf.lastIndex() < args.PrevLogIndex {
				reply.ConflictIndex = rf.lastIndex() + 1
				return
			}

			reply.ConflictIndex = args.PrevLogIndex
			reply.ConflictTerm = rf.Log[rf.index2LogPos(reply.ConflictIndex)].Term
			// 找到冲突term首次出现的位置
			for reply.ConflictIndex > rf.LastIncludedIndex && rf.Log[rf.index2LogPos(reply.ConflictIndex)].Term == reply.ConflictTerm {
				reply.ConflictIndex--
			}
			reply.ConflictIndex++
			_, _ = DPrintf("[S%d T%d] AppendEntries no match Log entry at prevLogIndex: %d, prevLogTerm: %d, replyIndex: %d)",
				rf.me, rf.CurrentTerm, args.PrevLogIndex, args.PrevLogTerm, reply.ConflictIndex)

			return
		}
	}

	if len(args.Entries) > 0 {
		for i, logEntry := range args.Entries {
			index := args.PrevLogIndex+1+i
			pos := rf.index2LogPos(index)
			if index <= rf.lastIndex() {
				if rf.Log[pos].Term != logEntry.Term {
					rf.Log = rf.Log[:pos]
					rf.Log = append(rf.Log, logEntry)
				}
			} else {
				rf.Log = append(rf.Log, logEntry)
			}
		}

		rf.persist()
	}

	//rf.lastApplied = len(rf.Log) - 1
	if len(args.Entries) > 0 {
		_, _ = DPrintf("[S%d T%d] AppendEntries new entry append, cur Log: %v", rf.me, rf.CurrentTerm, rf.Log)
	}

	if args.LeaderCommit > rf.commitIndex {
		old := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, rf.lastIndex())
		_, _ = DPrintf("[S%d T%d] AppendEntries update commitIndex, old: %d, new: %d", rf.me, rf.CurrentTerm, old, rf.commitIndex)
	}

}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	_, _ = DPrintf("[S%d T%d] installSnapshot, rf.LastIncludedIndex[%d], rf.LastIncludedTerm[%d], args.LastIncludedIndex[%d], "+
		"args.LastIncludedTerm[%d], len(rf.log)[%d]", rf.me, rf.CurrentTerm, rf.LastIncludedIndex, rf.LastIncludedTerm,
		args.LastIncludedIndex, args.LastIncludedTerm, len(rf.Log))

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		_, _ = DPrintf("[S%d T%d] installSnapshot early return because args.Term[%d] < rf.CurrentTerm[%d]",
			rf.me, rf.CurrentTerm, args.Term, rf.CurrentTerm)
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.state = Follower
		rf.VotedFor = -1
		rf.persist()
	}

	rf.lastTime = time.Now()

	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		_, _ = DPrintf("[S%d T%d] installSnapshot early return because args.LastIncludedIndex[%d] < rf.LastIncludedIndex[%d]",
			rf.me, rf.CurrentTerm, args.LastIncludedIndex, rf.LastIncludedIndex)
		return
	}
	// 因为log从1开始，重置log需要填一个空的保持坐标一致
	// update rf.commitIndex， 因为server会被snapshot覆盖，之前commit过的需要重新提交
	if args.LastIncludedIndex < rf.lastIndex() { // snapshot 比一部分新
		if rf.Log[rf.index2LogPos(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
			// term不一致，日志之后存在不同步，snapshot一定正确，舍弃所有日志
			rf.Log = make([]LogEntry, 1)
			rf.commitIndex = args.LastIncludedIndex
		} else {
			// 覆盖一部分日志
			remainLog := make([]LogEntry, len(rf.Log[rf.index2LogPos(args.LastIncludedIndex)+1:]))
			copy(remainLog, rf.Log[rf.index2LogPos(args.LastIncludedIndex)+1:])
			rf.Log = append([]LogEntry{{}}, remainLog...)
			rf.commitIndex = min(rf.commitIndex, args.LastIncludedIndex)
		}
	} else {
		// snapshot超过日志，全覆盖
		rf.Log = make([]LogEntry, 1)
		rf.commitIndex = min(rf.commitIndex, args.LastIncludedIndex)
	}

	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), args.Data)
	rf.installSnapshotToApplication()
	// update rf.lastApplied， 因为server会被snapshot覆盖，之前commit过的需要重新提交
	rf.lastApplied = args.LastIncludedIndex
	_, _ = DPrintf("[S%d T%d] installSnapshot end, rf.LastIncludedIndex[%d], rf.LastIncludedTerm[%d], args.LastIncludedIndex[%d], "+
		"args.LastIncludedTerm[%d], len(rf.log)[%d]", rf.me, rf.CurrentTerm, rf.LastIncludedIndex, rf.LastIncludedTerm,
		args.LastIncludedIndex, args.LastIncludedTerm, len(rf.Log))
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	index = rf.lastIndex() + 1
	rf.Log = append(rf.Log, LogEntry{
		Msg: ApplyMsg{
			Command:      command,
			CommandIndex: index,
			CommandTerm:  term,
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
	lastLogIndex := rf.lastIndex()
	lastLogTerm := rf.lastTerm()
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
		rf.nextIndex[i] = rf.lastIndex() + 1
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
		lastLogIndex := rf.lastIndex()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.nextIndex[i] <= rf.LastIncludedIndex {
				_, _ = DPrintf("[S%d T%d] check and will doInstallSnapshot to S%d, rf.nextIndex[%d] = [%d], rf.LastIncludedIndex[%d]",
					rf.me, rf.CurrentTerm, i, i, rf.nextIndex[i], rf.LastIncludedIndex)
				continue
			}
			prevLogIndex[i] = rf.nextIndex[i] - 1
			_, _ = DPrintf("[S%d T%d] replicateLog, S%d, prevLogIndex[%d], rf.LastIncludedIndex[%d], len(rf.Log)[%d]", rf.me, rf.CurrentTerm,
				i, prevLogIndex[i], rf.LastIncludedIndex, len(rf.Log))
			if prevLogIndex[i] == rf.LastIncludedIndex {
				prevLogTerm[i] = rf.LastIncludedTerm
			} else {
				prevLogTerm[i] = rf.Log[rf.index2LogPos(rf.nextIndex[i])-1].Term
			}
			if lastLogIndex >= rf.nextIndex[i] {
				_, _ = DPrintf("[S%d T%d] replicateLog to S%d, last: %d, next: %d", rf.me, term, i, lastLogIndex, rf.nextIndex[i])
				entry := make([]LogEntry, len(rf.Log[rf.index2LogPos(rf.nextIndex[i]):]))
				copy(entry, rf.Log[rf.index2LogPos(rf.nextIndex[i]):])
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

func (rf *Raft) canDoInstallSnapshot(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[peer] <= rf.LastIncludedIndex
}

// compare the current term with the term you sent in your original RPC. If the two are different, drop the reply and return
func (rf *Raft) broadcastAppendEntries(term, leaderCommit, lastLogIndex, count int, entries [][]LogEntry, prevLogIndex, 
	prevLogTerm []int) (bool, int) {

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
			if rf.canDoInstallSnapshot(i) {
				rf.mu.Lock()
				lastIncludedIndex := rf.LastIncludedIndex
				lastIncludedTerm := rf.LastIncludedTerm
				rf.mu.Unlock()
				rf.doInstallSnapshot(i, term, lastIncludedIndex, lastIncludedTerm, &r, done, stopCh)
				return
			}

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex[i],
				PrevLogTerm:  prevLogTerm[i],
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			reply := &AppendEntriesReply{}

			ok := rf.sendAppendEntries(i, args, reply)
			if !ok {
				select {
				case <-done:
					return
				default:
					_, _ = DPrintf("[S%d T%d] heartbeat lost to S%d", rf.me, term, i)
				}
				return
			}
			atomic.StoreInt32(&r, 1)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != Leader || rf.CurrentTerm != args.Term {
				//log.Printf("term not equal 2")

				return
			}

			if reply.Term > term {
				rf.CurrentTerm = reply.Term
				rf.state = Follower
				rf.VotedFor = -1
				rf.persist()
				select {
				case <-done:
					return
				case stopCh <- struct{}{}:
					_, _ = DPrintf("[S%d T%d] replicateLog response term bigger than now, become follower, term update to T%d, early stop", rf.me, term, reply.Term)

				}
				return
			}

			if reply.Success {
				if rf.nextIndex[i] > rf.matchIndex[i] {
					rf.matchIndex[i] = args.PrevLogIndex+len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					rf.updateCommitIndex()
					_, _ = DPrintf("[S%d T%d] replicateLog update follower S%d, next: %d, match: %d", rf.me, term, i, rf.nextIndex[i], rf.matchIndex[i])

				}
				return
			} else {
				/*
				quickly log backtrack process:
				If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
				If a follower does have prevLogIndex in its log, but the term does not match, it should return conflictTerm = log[prevLogIndex].Term,
				and then search its log for the first index whose entry has term equal to conflictTerm.

				Upon receiving a conflict response, the leader should first search its log for conflictTerm. If it finds an entry in its log with that term,
				it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
				If it does not find an entry with that term, it should set nextIndex = conflictIndex.
				*/
				if reply.ConflictTerm == -1 {
					rf.nextIndex[i] = reply.ConflictIndex
				} else {
					conflictIndex := -1
					for index := args.PrevLogIndex; index > rf.LastIncludedIndex; index-- {
						if rf.Log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
							conflictIndex = index
							break
						}
					}
					if conflictIndex != -1 {
						rf.nextIndex[i] = conflictIndex
					} else {
						rf.nextIndex[i] = reply.ConflictIndex
					}
				}
			}

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

func (rf *Raft) doInstallSnapshot(peer, term, lastIncludedIndex, lastIncludedTerm int, r *int32, done, stopCh chan struct{}) {
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
	_, _ = DPrintf("[S%d T%d] doInstallSnapshot to S%d, lastIncludedIndex[%d], lastIncludedTerm[%d], len(Data)[%d]",
		rf.me, term, peer, lastIncludedIndex, lastIncludedTerm, len(args.Data))

	reply := InstallSnapshotReply{}

	if rf.sendInstallSnapshot(peer, &args, &reply) {
		atomic.StoreInt32(r, 1)
		_, _ = DPrintf("[S%d T%d] doInstallSnapshot atomic r to %v", rf.me, term, atomic.LoadInt32(r))
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader || rf.CurrentTerm != args.Term {
			//log.Printf("term not equal 2")
			return
		}

		if reply.Term > args.Term {
			rf.state = Follower
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.persist()
			select {
			case <-done:
				return
			case stopCh <- struct{}{}:
				_, _ = DPrintf("[S%d T%d] doInstallSnapshot response term bigger than now, become follower, term update to T%d, early stop", rf.me, term, reply.Term)

			}
			return
		}
		// follower 成功返回后无法知道快照到哪一位，设置最后再回退
		rf.nextIndex[peer] = rf.lastIndex() + 1
		// rf.LastIncludedIndex rpc之后可能会变，以args的为准
		rf.matchIndex[peer] = args.LastIncludedIndex
		//if rf.LastIncludedIndex != args.LastIncludedIndex {
		//	log.Printf("[S%d T%d] rf.LastIncludedIndex[%d] != args.LastIncludedIndex[%d]", rf.me, rf.CurrentTerm, rf.LastIncludedIndex, args.LastIncludedIndex)
		//}
		rf.updateCommitIndex()
	} else {
		select {
		case <-done:
			return
		default:
			_, _ = DPrintf("[S%d T%d] doInstallSnapshot lost to S%d", rf.me, term, peer)
		}

		return
	}
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

func (rf *Raft) triggerApplyMsg() {
	for !rf.killed() {

		rf.mu.Lock()
		term := rf.CurrentTerm

		for rf.lastApplied < rf.commitIndex {
			_, _ = DPrintf("[S%d T%d] apply Log, lastApplied: %d, commitIndex: %d", rf.me, term, rf.lastApplied, rf.commitIndex)

			rf.lastApplied++
			appliedPos := rf.index2LogPos(rf.lastApplied)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[appliedPos].Msg.Command,
				CommandIndex: rf.lastApplied,
				CommandTerm:  rf.Log[appliedPos].Msg.CommandTerm,
			}
			_, _ = DPrintf("[S%d T%d] send msg back to applyCh, %#v", rf.me, term, msg)
			rf.applyCh <- msg
		}
		rf.mu.Unlock()

		//_, _ = DPrintf("[S%d T%d] triggerApplyMsg sleep", rf.me, term)
		time.Sleep(replyMsgInterval)
	}
}

func (rf *Raft) updateCommitIndex() {
	newCommitIndex := rf.commitIndex
	for n := rf.commitIndex + 1; n < rf.lastIndex()+1; n++ {
		if rf.index2LogPos(n) < 0 { // rf.LastIncludedIndex or rf.Log may change after rpc
			continue
		}
		if rf.Log[rf.index2LogPos(n)].Term != rf.CurrentTerm {
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

	//matchSlice := make([]int, 0, len(rf.peers))
	//for i := 0; i < len(rf.peers); i++ {
	//	if i == rf.me {
	//		matchSlice = append(matchSlice, rf.lastIndex())
	//	}
	//	matchSlice = append(matchSlice, rf.matchIndex[i])
	//}
	//sort.Ints(matchSlice)
	//newCommitIndex := matchSlice[len(rf.peers)/2]
	//valid := newCommitIndex <= rf.LastIncludedIndex || rf.Log[rf.index2LogPos(newCommitIndex)].Term == rf.CurrentTerm

	if newCommitIndex > rf.commitIndex {
		_, _ = DPrintf("[S%d T%d] leader commitIndex update %d -> %d", rf.me, rf.CurrentTerm, rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
	}
}

func (rf *Raft) TakeSnapshot(snapshot []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.LastIncludedIndex {
		return
	}

	_, _ = DPrintf("[S%d T%d] TakeSnapshot begins, isLeader[%t], snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.CurrentTerm, rf.state == Leader, lastIncludedIndex, rf.LastIncludedIndex, rf.LastIncludedTerm)



	afterLog := make([]LogEntry, len(rf.Log[rf.index2LogPos(lastIncludedIndex)+1:]))
	copy(afterLog, rf.Log[rf.index2LogPos(lastIncludedIndex)+1:])

	// 以原有log获取lastIncludedIndex的term，然后截断log
	rf.LastIncludedTerm = rf.Log[rf.index2LogPos(lastIncludedIndex)].Term
	rf.LastIncludedIndex = lastIncludedIndex
	rf.Log = append([]LogEntry{{}}, afterLog...)

	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), snapshot)

	_, _ = DPrintf("[S%d T%d] TakeSnapshot end, isLeader[%t], snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.CurrentTerm, rf.state == Leader, lastIncludedIndex, rf.LastIncludedIndex, rf.LastIncludedTerm)
}

func (rf *Raft) ExceedLogSize(logSize int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.persister.RaftStateSize() >= logSize {
		return true
	}
	return false
}

func (rf *Raft) installSnapshotToApplication() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	applyMsg := ApplyMsg{
		CommandValid:      false,
		Snapshot:          rf.persister.ReadSnapshot(),
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
	}

	rf.lastApplied = rf.LastIncludedIndex

	_, _ = DPrintf("[S%d] installSnapshotToApplication, size[%d], lastIncludedIndex[%d], lastIncludedTerm[%d]",
		rf.me, len(applyMsg.Snapshot), applyMsg.LastIncludedIndex, applyMsg.LastIncludedTerm)
	rf.applyCh <- applyMsg
	return
}

func (rf *Raft) lastIndex() int {
	return rf.LastIncludedIndex + len(rf.Log) - 1
}

func (rf *Raft) lastTerm() int {
	lastLogTerm := rf.LastIncludedTerm
	if len(rf.Log) > 1 {
		lastLogTerm = rf.Log[len(rf.Log)-1].Term
	}
	return lastLogTerm
}

func (rf *Raft) index2LogPos(index int) int {
	return index - rf.LastIncludedIndex
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
	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Lock()
	rf.installSnapshotToApplication()
	rf.mu.Unlock()

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

	go rf.triggerApplyMsg()
	return rf
}
