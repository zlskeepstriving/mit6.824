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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
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
	randTime           = 300
	heartbeatPeriod    = 120
	electionTimerName  = "electionTimer"
	heartbeatTimerName = "heartbeatTimer"
)

var (
	rander          = rand.New(rand.NewSource(time.Now().UnixNano()))
	electionTimeout int
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

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond   //used to wake up applier goroutine after committing new entries
	replicatorCond []*sync.Cond //used to signal replicator goroutine to batch replicating entries
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	leaderID       int
	iedentity      Identity
	CurrentTerm    int
	VotedFor       int
	Logs           []LogEntry
	commitedIndex  int
	lastApplied    int
	nextIndex      []int
	matchIndex     []int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.CurrentTerm
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf("[persist]: peer[%d] Term[%d] Identity[%s] save persistent state", rf.me, rf.CurrentTerm, rf.getIdentity())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Logs []LogEntry
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(Logs) != nil {
		DPrintf("[readPersist]: Decode error")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Logs = Logs
	}
	DPrintf("[readPersist]: peer[%d] Term[%d] Identity[%s] read persistent state from Persister", rf.me, rf.CurrentTerm, rf.getIdentity())
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
	return rf.Logs[len(rf.Logs)-1].Index
}

func (rf *Raft) resetTimer(timerName string) {
	switch timerName {
	case "electionTimer":
		electionTimeout = rander.Intn(150) + randTime
		rf.electionTimer.Reset(time.Millisecond * time.Duration(electionTimeout))
	case "heartbeatTimer":
		rf.heartbeatTimer.Reset(time.Millisecond * time.Duration(heartbeatPeriod))
	default:
		log.Fatal("unexpected timerName\n")
	}
}

func (rf *Raft) isMoreUpToDate(args *RequestVoteArgs) bool {
	lastIndex := rf.getLastIndexOfLogs()
	if rf.Logs[lastIndex].Term > args.LastLogTerm || (rf.Logs[lastIndex].Term == args.LastLogTerm && lastIndex > args.LastLogIndex) {
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
	DPrintf("[RequestVote]: peer[%v] identity[%v] receive requestVote RPC: term[%v], candidateID[%v], CurrentTerm[%v], VotedFor[%v]", rf.me, rf.getIdentity(), args.Term, args.CandidateID, rf.CurrentTerm, rf.VotedFor)
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	if args.Term < rf.CurrentTerm || (args.Term == rf.CurrentTerm && rf.VotedFor != -1 && rf.VotedFor != args.CandidateID) {
		DPrintf("[RequestVote rejection]: peer[%d]'s CurrentTerm is larger than peer[%d]", rf.me, args.CandidateID)
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.changeTo(Follower)
		rf.CurrentTerm = args.Term
		rf.persist()
	}
	if rf.isMoreUpToDate(args) {
		DPrintf("[RequestVote rejection]: peer[%d]'s log is more update than peer[%d]", rf.me, args.CandidateID)
		return
	}
	rf.CurrentTerm, rf.VotedFor = args.Term, args.CandidateID
	rf.persist()
	rf.resetTimer(electionTimerName)
	reply.Term, reply.VoteGranted = rf.CurrentTerm, true
	//DPrintf("term[%v], peer[%v] vote for peer[%v]\n", rf.CurrentTerm, rf.me, args.CandidateID)
}

func (rf *Raft) checkLogConsistency(args *AppendEntriesArgs) bool {
	if rf.getLastIndexOfLogs() < args.PrevLogIndex {
		return false
	}
	return rf.Logs[args.PrevLogIndex].Term == args.PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.CurrentTerm = rf.CurrentTerm
	reply.Success = false
	DPrintf("[AppendEntries]: peer[%d] receive a RPC from leader peer[%d], args:[%v]", rf.me, args.LeaderID, args)
	//check term and log match
	if args.Term < rf.CurrentTerm {
		DPrintf("[AppendEntries]: peer[%d] reject peer[%d]'s RPC, because term[%d] is larger than args.Term[%d]", rf.me, args.LeaderID, rf.CurrentTerm, args.Term)
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}
	//DPrintf("term[%d], peer[%d] receive an AppendEntries RPC from Leader peer[%d]", args.Term, rf.me, args.LeaderID)
	rf.changeTo(Follower)
	rf.resetTimer(electionTimerName)

	if !rf.checkLogConsistency(args) {
		DPrintf("[AppendEntries]: peer[%d] reject peer[%d]'s RPC because of log inConsistency!", rf.me, args.LeaderID)
		return
	}

	if len(rf.Logs) > 1 {
		DPrintf("[AppendEntries]: peer[%d] old Logs:[%v], entries:[%v]", rf.me, rf.Logs[1:], args.Entries)
	}

	//find the latest non-match log, delete all log follow it and append new entry to it's log
	logIndex := args.PrevLogIndex + 1
	for entriesIndex, entry := range args.Entries {
		if logIndex >= len(rf.Logs) || rf.Logs[logIndex].Term != entry.Term {
			rf.Logs = append(rf.Logs[:logIndex], args.Entries[entriesIndex:]...)
			break
		}
		logIndex += 1
	}
	rf.persist()

	//update follower's commitedIndex = min(LeaderCommit, indexofLastEntry)
	DPrintf("[AppendEntries]: peer[%d] current Logs:[%v], [Leader Commit]: %d", rf.me, rf.Logs[1:], args.LeaderCommit)
	indexOfLastEntry := rf.Logs[len(rf.Logs)-1].Index
	if args.LeaderCommit > rf.commitedIndex {
		rf.commitedIndex = args.LeaderCommit
		if indexOfLastEntry < rf.commitedIndex {
			rf.commitedIndex = indexOfLastEntry
		}
	}
	go rf.applyEntry()

	rf.leaderID = args.LeaderID
	reply.CurrentTerm = rf.CurrentTerm
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

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.iedentity == Leader {
				rf.mu.Unlock()
				continue
			}
			rf.changeTo(Candidate)
			rf.VotedFor = rf.me
			rf.CurrentTerm += 1
			rf.persist()
			rf.startElection()
			rf.resetTimer(electionTimerName)
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if _, isLeader := rf.GetState(); isLeader {
				rf.broadcastAppendEntries(true)
				rf.resetTimer(heartbeatTimerName)
			}
			rf.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 10)
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

func (rf *Raft) startElection() {
	DPrintf("[StartElection] time out [%d]ms, term[%d], peer[%d] start to elect as a candidate", electionTimeout, rf.CurrentTerm, rf.me)
	var voteNum int = 1 // first, vote for self

	for serverID := 0; serverID < len(rf.peers); serverID++ {
		if serverID == rf.me {
			continue
		}
		go func(serverID int) {
			rf.mu.Lock()
			if rf.iedentity != Candidate {
				rf.mu.Unlock()
				return
			}
			args := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateID:  rf.me,
				LastLogIndex: rf.Logs[len(rf.Logs)-1].Index,
				LastLogTerm:  rf.Logs[len(rf.Logs)-1].Term,
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(serverID, &args, &reply); ok {
				rf.mu.Lock()
				if rf.CurrentTerm == args.Term && rf.iedentity == Candidate {
					if reply.VoteGranted {
						DPrintf("term[%d], peer[%d] vote for peer[%d]", rf.CurrentTerm, serverID, rf.me)
						voteNum += 1
						if voteNum > len(rf.peers)/2 {
							DPrintf("term[%d], peer[%d] become Leader", rf.CurrentTerm, rf.me)
							rf.changeTo(Leader)
							rf.leaderID = rf.me
							//init nextIndex for each peer
							for i := 0; i < len(rf.peers); i++ {
								if i == rf.me {
									continue
								}
								rf.matchIndex[i] = 0
								rf.nextIndex[i] = rf.Logs[len(rf.Logs)-1].Index + 1
							}
							rf.broadcastAppendEntries(true)
							rf.resetTimer(heartbeatTimerName)
						}
					} else if reply.Term > rf.CurrentTerm {
						DPrintf("peer[%d] reject to vote for peer[%d], because term[%d] is smaller than term[%d]", serverID, rf.me, rf.CurrentTerm, reply.Term)
						rf.changeTo(Follower)
						rf.CurrentTerm, rf.VotedFor = reply.Term, -1
						rf.persist()
					}
				}
				rf.mu.Unlock()
			}
		}(serverID)
	}
}

func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	if rf.killed() || rf.iedentity != Leader {
		return
	}
	replicateNum := 1
	successOnce := false

	for serverID := 0; serverID < len(rf.peers); serverID++ {
		if serverID == rf.me {
			continue
		}
		go func(serverID int, isHeartbeat bool) {
		retry:
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.nextIndex[serverID] - 1,
				PrevLogTerm:  rf.Logs[rf.nextIndex[serverID]-1].Term,
				Entries:      rf.Logs[rf.nextIndex[serverID]:],
				LeaderCommit: rf.commitedIndex,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(serverID, &args, &reply); ok {
				DPrintf("[broadcastAppendEntries]: leader peer[%d] send RPC[%v] to peer[%d]", rf.me, args, serverID)
				rf.mu.Lock()
				if args.Term != rf.CurrentTerm || rf.iedentity != Leader {
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					rf.nextIndex[serverID] = rf.Logs[len(rf.Logs)-1].Index + 1
					rf.matchIndex[serverID] = rf.Logs[len(rf.Logs)-1].Index
					if !isHeartbeat {
						replicateNum += 1
						if replicateNum > len(rf.peers)/2 && !successOnce {
							successOnce = true
							rf.commitedIndex = rf.Logs[len(rf.Logs)-1].Index
							go rf.applyEntry()
						}
					}
				} else {
					if reply.CurrentTerm > rf.CurrentTerm {
						DPrintf("[broadcastAppendEntries]: peer[%d]'s term[%d] is larger than peer[%d]'s term[%d], change to follower", serverID, reply.CurrentTerm, rf.me, rf.CurrentTerm)
						rf.changeTo(Follower)
						rf.CurrentTerm = reply.CurrentTerm
						rf.VotedFor = -1
						rf.persist()
					} else {
						DPrintf("[broadcastAppendEntries]: peer[%d]'s log doesn't match with Leader peer[%d] log[%d], so decrease nextIndex and retry",
							serverID, rf.me, rf.nextIndex[serverID])
						if rf.nextIndex[serverID] > 1 {
							rf.nextIndex[serverID] -= 1
						}
						rf.mu.Unlock()
						goto retry
					}
				}
				rf.mu.Unlock()
			} else {
				goto retry
			}
		}(serverID, isHeartbeat)
	}

	time.Sleep(time.Millisecond * 10)
	go rf.updateCommitedIndex()
}

func (rf *Raft) updateCommitedIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for n := rf.commitedIndex + 1; n < rf.getLastIndexOfLogs(); n += 1 {
		if _, isLeader := rf.GetState(); !isLeader {
			return
		}
		cnt := 1
		for i := 0; i < len(rf.peers); i += 1 {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= n {
				cnt += 1
			}
		}
		if cnt > len(rf.peers)/2 && rf.Logs[n].Term == rf.CurrentTerm {
			rf.commitedIndex = n
		} else {
			break
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		commitedIndex := rf.commitedIndex
		lastApplied := rf.lastApplied
		rf.mu.Unlock()

		if lastApplied == commitedIndex {
			rf.mu.Lock()
			rf.applyCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			rf.applyEntry()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) replicateOneRound(serverID int) {
	rf.mu.Lock()
	if rf.iedentity != Leader {
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) replicator(serverID int) {
	rf.replicatorCond[serverID].L.Lock()
	defer rf.replicatorCond[serverID].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(serverID) {
			rf.replicatorCond[serverID].Wait()
		}
		rf.replicateOneRound(serverID)
	}
}

func (rf *Raft) needReplicating(serverID int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.iedentity == Leader && rf.matchIndex[serverID] < rf.Logs[rf.getLastIndexOfLogs()].Index
}

//TODO: 修改applyEntry，循环处理，直到applyIndex = commitedIndex

func (rf *Raft) applyEntry() {
	rf.mu.Lock()
	lastApplied := rf.lastApplied
	commitedIndex := rf.commitedIndex
	rf.mu.Unlock()

	for lastApplied < commitedIndex {
		DPrintf("[AppendEntries]: peer[%d] lastApplied[%d] commitedIndex[%d]", rf.me, lastApplied, commitedIndex)
		lastApplied += 1
		newApplyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Logs[lastApplied].Command,
			CommandIndex: rf.Logs[lastApplied].Index,
		}
		rf.applyCh <- newApplyMsg
		DPrintf("[applyEntry]: peer[%d] apply entry[%v] to state machine", rf.me, rf.Logs[lastApplied])
	}
}

func (rf *Raft) insertLog(command interface{}) (int, int) {
	term := rf.CurrentTerm
	index := rf.Logs[len(rf.Logs)-1].Index + 1
	newEntry := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.Logs = append(rf.Logs, newEntry)
	rf.persist()
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
	go rf.broadcastAppendEntries(false)
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
	electionTimeout = rander.Intn(150) + randTime
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		iedentity:      Follower,
		CurrentTerm:    0,
		VotedFor:       -1,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		Logs:           make([]LogEntry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		commitedIndex:  0,
		lastApplied:    0,
		electionTimer:  time.NewTimer(time.Millisecond * time.Duration(electionTimeout)),
		heartbeatTimer: time.NewTimer(time.Millisecond * heartbeatPeriod),
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.Logs[0] = LogEntry{0, 0, nil}
	rf.applyCond = sync.NewCond(&rf.mu)

	/*
		for serverID := 0; serverID < len(peers); serverID++ {
			if serverID == rf.me {
				continue
			}
			rf.replicatorCond[serverID] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(serverID)
		}
	*/

	go rf.ticker()
	//go rf.applier()
	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	return rf
}
