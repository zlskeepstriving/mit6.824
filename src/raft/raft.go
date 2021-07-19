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
	Candiate
	Follower
)

const (
	randTime = 200
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

type entry struct {
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

	timer         *time.Timer
	timeout       int
	leaderID      int
	iedentity     Identity
	currentTerm   int
	votedFor      int
	log           []*entry
	commitedIndex int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
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
	CurrentTerm int
	VoteGranted bool
}

//the RPC for leader to appending log entry and heartbeat
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*entry
	LeaderCommit int //leader's commitIndex
	IsHeartBeat  bool
}

type AppendEntriesReply struct {
	CurrentTerm int
	Success     bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.timeout = rander.Intn(150) + randTime
	rf.timer.Reset(time.Millisecond * time.Duration(rf.timeout)) //reset timer

	if args.Term <= rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		reply.VoteGranted = false
		return
	} else {
		rf.currentTerm = args.Term
		rf.iedentity = Follower
	}

	rf.votedFor = args.CandidateID
	reply.CurrentTerm = rf.currentTerm
	reply.VoteGranted = true
	DPrintf("term: [%v], peer[%v] vote for peer[%v]\n", rf.currentTerm, rf.me, args.CandidateID)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if rf.currentTerm > args.Term {
		reply.CurrentTerm = rf.currentTerm
		reply.Success = false
		return
	} else {
		rf.currentTerm = args.Term
		rf.iedentity = Follower
	}

	rf.timeout = rander.Intn(150) + randTime
	rf.timer.Reset(time.Millisecond * time.Duration(rf.timeout)) //reset timer

	rf.leaderID = args.LeaderID
	reply.CurrentTerm = rf.currentTerm
	reply.Success = true
	if args.IsHeartBeat {
		DPrintf("term[%v], peer[%v] has received heartbeat from leader peer[%v]", rf.currentTerm, rf.me, args.LeaderID)
		return
	} else {
		rf.log = append(rf.log, args.Entries...)
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

//Start the election

func (rf *Raft) startElection() {
	if rf.iedentity == Follower {
		//time out , now start to elect as a candidate
		DPrintf("peer[%v] timer[%v] is out, now start to elect as a candidate", rf.me, rf.timeout)

		resultChan := make(chan bool, len(rf.peers))
		defer close(resultChan)
		rf.iedentity = Candiate
		rf.votedFor = rf.me
		var voteNum int = 1 // first, vote for self
		rf.currentTerm++

		for serverID := 0; serverID < len(rf.peers); serverID++ {
			if serverID != rf.me {
				go func(serverID int) {
					args := RequestVoteArgs{
						Term:        rf.currentTerm,
						CandidateID: rf.me,
					}
					reply := RequestVoteReply{}
					rf.sendRequestVote(serverID, &args, &reply)
					resultChan <- reply.VoteGranted
				}(serverID)

			}
			//DPrintf("term :%v, candidate peer[%v] has received %v votes", rf.currentTerm, rf.me, voteNum)
		}
		for {
			result := <-resultChan
			if result {
				voteNum++
			}
			if voteNum > len(rf.peers)/2 {
				rf.leaderID = rf.me
				rf.iedentity = Leader
				DPrintf("peer[%v] is elected as a leader, now start serving...", rf.me)
				rf.serveAsLeader()
				break
			}
		}
	}
}

func (rf *Raft) serveAsLeader() {
	heartBeatTimeOut := 150
	for {
		if rf.iedentity != Leader {
			break
		}
		for serverID := 0; serverID < len(rf.peers); serverID++ {
			if serverID != rf.me {
				go func(serverID int) {
					args := AppendEntriesArgs{
						Term:        rf.currentTerm,
						LeaderID:    rf.me,
						IsHeartBeat: true,
					}
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(serverID, &args, &reply)
				}(serverID)
			}
		}
		time.Sleep(time.Millisecond * time.Duration(heartBeatTimeOut))
	}
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
	rf.timeout = rander.Intn(150) + randTime
	rf.timer = time.NewTimer(time.Millisecond * time.Duration(rf.timeout))

	go func() {
		for {
			<-rf.timer.C
			rf.startElection()
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
