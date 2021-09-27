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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

type Identity int

const (
	Leader Identity = iota
	Candidate
	Follower
)

const (
	randTime = 240
)

var (
	rander = rand.New(rand.NewSource(time.Now().UnixNano()))
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

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
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

	applyCh         chan ApplyMsg
	applyCond       *sync.Cond
	electionTimer   *time.Timer
	heartbeatTimer  *time.Timer
	electionTimeout int
	heartbeatPeriod int
	leaderID        int
	iedentity       Identity
	currentTerm     int
	votedFor        int
	logs            []LogEntry
	commitedIndex   int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.iedentity == Leader {
		isleader = true
	} else {
		isleader = false
	}

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
	Term         int //candidate's term
	CandidateID  int
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

//the RPC for leader to appending log entry and heartbeat
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int //index of Log entry immediately preceding new ones
	PrevLogTerm  int //term of PrevLogIndex entry
	Entries      []LogEntry
	LeaderCommit int //leader's commitIndex
}

type AppendEntriesReply struct {
	CurrentTerm int
	Success     bool
}

func (rf *Raft) getLastIndexOfLogs() int {
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimeout = rander.Intn(150) + randTime
	rf.electionTimer.Reset(time.Millisecond * time.Duration(rf.electionTimeout))
}

func (rf *Raft) isMoreUpToDate(args *RequestVoteArgs) bool {
	lastIndex := rf.getLastIndexOfLogs()
	if rf.logs[lastIndex].Term > args.LastLogTerm || (rf.logs[lastIndex].Term == args.LastLogTerm && lastIndex > args.LastLogIndex) {
		return true
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[RequestVote]: peer[%v] identity[%v] receive requestVote RPC: term[%v], candidateID[%v], currentTerm[%v], votedFor[%v]", rf.me, rf.getIdentity(), args.Term, args.CandidateID, rf.currentTerm, rf.votedFor)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateID) {
		DPrintf("[RequestVote rejection]: peer[%d]'s currentTerm is larger than peer[%d]", rf.me, args.CandidateID)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	if rf.isMoreUpToDate(args) {
		DPrintf("[RequestVote rejection]: peer[%d]'s log is more update than peer[%d]", rf.me, args.CandidateID)
		return
	}
	rf.changeTo(Follower)
	rf.currentTerm, rf.votedFor = args.Term, args.CandidateID
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	//DPrintf("term[%v], peer[%v] vote for peer[%v]\n", rf.currentTerm, rf.me, args.CandidateID)
	rf.resetElectionTimer()
}

func (rf *Raft) checkLogConsistency(args *AppendEntriesArgs) bool {
	if rf.getLastIndexOfLogs() < args.PrevLogIndex {
		return false
	}
	return rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.CurrentTerm = rf.currentTerm
	reply.Success = false
	DPrintf("[AppendEntries]: peer[%d] receive a RPC from leader peer[%d], args:[%v]", rf.me, args.LeaderID, args)
	//check term and log match
	if args.Term < rf.currentTerm {
		DPrintf("[AppendEntries]: peer[%d] reject peer[%d]'s RPC, because term[%d] is larger than args.Term[%d]", rf.me, args.LeaderID, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	//DPrintf("term[%d], peer[%d] receive an AppendEntries RPC from Leader peer[%d]", args.Term, rf.me, args.LeaderID)
	rf.changeTo(Follower)
	rf.resetElectionTimer()

	if !rf.checkLogConsistency(args) {
		DPrintf("[AppendEntries]: peer[%d] reject peer[%d]'s RPC because of log inConsistency!", rf.me, args.LeaderID)
		return
	}

	if len(rf.logs) > 1 {
		DPrintf("[AppendEntries]: peer[%d] old logs:[%v], entries:[%v]", rf.me, rf.logs[1:], args.Entries)
	}

	//find the latest non-match log, delete all log follow it and append new entry to it's log
	logIndex := args.PrevLogIndex + 1
	for entriesIndex, entry := range args.Entries {
		if logIndex >= len(rf.logs) || rf.logs[logIndex].Term != entry.Term {
			rf.logs = append(rf.logs[:logIndex], args.Entries[entriesIndex:]...)
			break
		}
		logIndex += 1
	}

	DPrintf("[AppendEntries]: peer[%d] current logs:[%v], [Leader Commit]: %d", rf.me, rf.logs[1:], args.LeaderCommit)
	indexOfLastNewEntry := rf.logs[len(rf.logs)-1].Index
	if args.LeaderCommit > rf.commitedIndex {
		rf.commitedIndex = args.LeaderCommit
		if indexOfLastNewEntry < args.LeaderCommit {
			rf.commitedIndex = indexOfLastNewEntry
		}
	}
	rf.applyEntry()

	rf.leaderID = args.LeaderID
	reply.CurrentTerm = rf.currentTerm
	reply.Success = true
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

func (rf *Raft) eventLoop() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.iedentity == Leader {
				rf.mu.Unlock()
				continue
			}
			rf.changeTo(Candidate)
			rf.votedFor = rf.me
			rf.currentTerm += 1
			rf.startElection()
			rf.resetElectionTimer()
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if _, isLeader := rf.GetState(); isLeader {
				rf.broadcastHeartBeat()
				rf.heartbeatTimer.Reset(time.Millisecond * time.Duration(rf.heartbeatPeriod))
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) changeTo(newIdentity Identity) {
	rf.iedentity = newIdentity
}

func (rf *Raft) getIdentity() string {
	var identity string
	if rf.iedentity == Leader {
		identity = "Leader"
	} else if rf.iedentity == Candidate {
		identity = "Candidate"
	} else if rf.iedentity == Follower {
		identity = "Follower"
	} else {
		identity = "Invalid"
	}
	return identity
}

//Start the election

func (rf *Raft) startElection() {
	DPrintf("[StartElection] time out [%d]ms, term[%d], peer[%d] start to elect as a candidate", rf.electionTimeout, rf.currentTerm, rf.me)
	var voteNum int = 1 // first, vote for self
	var winThreshold = len(rf.peers)/2 + 1

	for serverID := 0; serverID < len(rf.peers); serverID++ {
		if serverID == rf.me {
			continue
		}
		go func(serverID int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: rf.logs[len(rf.logs)-1].Index,
				LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
		retry:
			if ok := rf.sendRequestVote(serverID, &args, &reply); ok {
				rf.mu.Lock()
				if rf.currentTerm == reply.Term && rf.iedentity == Candidate {
					if reply.VoteGranted {
						DPrintf("term[%d], peer[%d] vote for peer[%d]", rf.currentTerm, serverID, rf.me)
						voteNum += 1
						if voteNum >= winThreshold {
							DPrintf("term[%d], peer[%d] become Leader", rf.currentTerm, rf.me)
							rf.changeTo(Leader)
							rf.leaderID = rf.me
							//init nextIndex for each peer
							for i := 0; i < len(rf.peers); i++ {
								rf.matchIndex[i] = 0
								rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index + 1
							}
							rf.broadcastHeartBeat()
							rf.heartbeatTimer.Reset(time.Millisecond * time.Duration(rf.heartbeatPeriod))
						}
					}
				} else if reply.Term > rf.currentTerm {
					DPrintf("peer[%d] reject to vote for peer[%d], because term[%d] is smaller than term[%d]", serverID, rf.me, rf.currentTerm, reply.Term)
					rf.changeTo(Follower)
					rf.currentTerm, rf.votedFor = reply.Term, -1
				}
				rf.mu.Unlock()
			} else {
				goto retry
			}
		}(serverID)
	}
}

func (rf *Raft) broadcastHeartBeat() {
	if rf.killed() || rf.iedentity != Leader {
		return
	}
	DPrintf("[broadcastHeartBeat]: Leader peer[%d] start to broadcast heartbeat to other peers", rf.me)
	for serverID := 0; serverID < len(rf.peers); serverID++ {
		if serverID == rf.me {
			continue
		}
		go func(serverID int) {
		retry:
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.logs[rf.nextIndex[serverID]-1].Index,
				PrevLogTerm:  rf.logs[rf.nextIndex[serverID]-1].Term,
				Entries:      rf.logs[rf.nextIndex[serverID]:],
				LeaderCommit: rf.commitedIndex,
			}
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(serverID, &args, &reply); ok {
				if reply.Success {
					rf.nextIndex[serverID] = rf.logs[len(rf.logs)-1].Index + 1
					rf.matchIndex[serverID] = rf.logs[len(rf.logs)-1].Index
				} else {
					if reply.CurrentTerm > rf.currentTerm {
						rf.changeTo(Follower)
						rf.currentTerm = reply.CurrentTerm
						rf.votedFor = -1
					} else {
						DPrintf("[replicateNewEntry]: peer[%d]'s log doesn't match with Leader peer[%d] log[%d], so decrease nextIndex and retry",
							serverID, rf.me, rf.nextIndex[serverID])
						if rf.nextIndex[serverID] > 1 {
							rf.nextIndex[serverID] -= 1
							goto retry
						}
					}
				}
			} else {
				goto retry
			}
		}(serverID)
	}

	end := false
	for n := len(rf.logs) - 1; n > rf.commitedIndex; n -= 1 {
		cnt := 0
		for i := 0; i < len(rf.peers); i += 1 {
			if rf.matchIndex[i] >= n {
				cnt += 1
			}
			if cnt >= len(rf.peers)/2+1 && rf.logs[n].Term == rf.currentTerm {
				rf.commitedIndex = n
				end = true
				break
			}
		}
		if end {
			break
		}
	}
}

func (rf *Raft) replicateNewEntry() {
	if rf.killed() || rf.iedentity != Leader {
		return
	}
	DPrintf("[replicateNewEntry]: Leader peer[%d] start to replicate new entry", rf.me)
	replicateNum := 1
	majorityNum := len(rf.peers)/2 + 1
	successOnce := false

	for serverID := 0; serverID < len(rf.peers); serverID++ {
		if serverID == rf.me {
			continue
		}
		go func(serverID int) {
		retry:
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.logs[rf.nextIndex[serverID]-1].Index,
				PrevLogTerm:  rf.logs[rf.nextIndex[serverID]-1].Term,
				Entries:      rf.logs[rf.nextIndex[serverID]:],
				LeaderCommit: rf.commitedIndex,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(serverID, &args, &reply); ok {
				//DPrintf("[replicateNewEntry]: Leader peer[%d] send entry to peer[%d]", rf.me, serverID)
				rf.mu.Lock()
				if reply.Success {
					//DPrintf("[replicateNewEntry]: Leader peer[%d] replicate log[%d] on peer[%d] successfully", rf.me, rf.nextIndex[serverID]-1, serverID)
					rf.nextIndex[serverID] = rf.logs[len(rf.logs)-1].Index + 1
					rf.matchIndex[serverID] = rf.logs[len(rf.logs)-1].Index
					replicateNum += 1
					if replicateNum >= majorityNum && !successOnce {
						successOnce = true
						rf.commitedIndex = rf.logs[len(rf.logs)-1].Index
						rf.applyEntry()
						rf.broadcastHeartBeat()
						rf.heartbeatTimer.Reset(time.Millisecond * time.Duration(rf.heartbeatPeriod))
					}
					rf.mu.Unlock()
				} else if reply.CurrentTerm > rf.currentTerm {
					DPrintf("[replicateNewEntry]: peer[%d] reject to replicate log[%d], because currentTerm[%d] is smaller than term[%d]",
						serverID, rf.nextIndex[serverID], rf.currentTerm, reply.CurrentTerm)
					rf.changeTo(Follower)
					rf.currentTerm = reply.CurrentTerm
					rf.votedFor = -1
					rf.mu.Unlock()
				} else {
					DPrintf("[replicateNewEntry]: peer[%d]'s log doesn't match with Leader peer[%d] log[%d], so decrease nextIndex and retry",
						serverID, rf.me, rf.nextIndex[serverID])
					if rf.nextIndex[serverID] > 1 {
						rf.nextIndex[serverID] -= 1
					}
					rf.mu.Unlock()
					goto retry
				}
			} else {
				goto retry
			}
		}(serverID)
	}

	end := false
	for n := len(rf.logs) - 1; n > rf.commitedIndex; n -= 1 {
		cnt := 0
		for i := 0; i < len(rf.peers); i += 1 {
			if rf.matchIndex[i] >= n {
				cnt += 1
			}
			if cnt >= len(rf.peers)/2+1 && rf.logs[n].Term == rf.currentTerm {
				rf.commitedIndex = n
				end = true
				break
			}
		}
		if end {
			break
		}
	}
}

//TODO: 修改applyEntry，循环处理，直到applyIndex = commitedIndex

func (rf *Raft) applyEntry() {
	for rf.lastApplied < rf.commitedIndex {
		DPrintf("[AppendEntries]: peer[%d] lastApplied[%d] commitedIndex[%d]", rf.me, rf.lastApplied, rf.commitedIndex)
		rf.lastApplied += 1
		newApplyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.logs[rf.lastApplied].Index,
		}
		rf.applyCh <- newApplyMsg
		DPrintf("[applyEntry]: peer[%d] apply entry[%v] to state machine", rf.me, rf.logs[rf.lastApplied])
	}
}

func (rf *Raft) insertLog(command interface{}) (int, int) {
	term := rf.currentTerm
	index := rf.logs[len(rf.logs)-1].Index + 1
	newEntry := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.logs = append(rf.logs, newEntry)
	DPrintf("leader peer[%d] append log at index[%d] in term[%d]", rf.me, index, term)
	return term, newEntry.Index
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
	rf.mu.Lock()
	if rf.iedentity != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	DPrintf("[Start], command[%v]", command)
	term, index = rf.insertLog(command)
	rf.mu.Unlock()
	rf.replicateNewEntry()
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
	rf.iedentity = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.electionTimeout = rander.Intn(150) + randTime
	rf.electionTimer = time.NewTimer(time.Millisecond * time.Duration(rf.electionTimeout))
	rf.heartbeatPeriod = 120
	rf.heartbeatTimer = time.NewTimer(time.Millisecond * time.Duration(rf.heartbeatPeriod))

	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	rf.logs = make([]LogEntry, 1)
	rf.logs[0] = LogEntry{0, 0, nil}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitedIndex = 0
	rf.lastApplied = 0

	go rf.eventLoop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
