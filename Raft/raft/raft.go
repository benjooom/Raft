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
	//	"bytes"

	"bytes"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Log struct storing information about log entries
type LogRecord struct {
	Term    int         // term in which entry was created
	Command interface{} // command for state machine
}

// Create Raft state variable
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int         // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int         // candidateId that received vote in current term (or null if none)
	log         []LogRecord // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// Additional state variables
	applyCh   chan ApplyMsg // channel on which to send ApplyMsg messages
	state     State         // current state of the server
	heartbeat bool          // keeps track of heartbeats
	voteCount int           // keeps track of votes received
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	//Lock raft
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogRecord
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		return
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry (§5.4)
	LastLogTerm  int // term of candidate's last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// Lock raft
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteCount = 0
		rf.state = Follower
		rf.persist()
	}

	// Set reply term to current term
	reply.Term = rf.currentTerm

	// If votedFor is null or candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && (args.LastLogIndex+1) >= len(rf.log))) {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.voteCount = 0
		rf.state = Follower
		rf.persist()
	}

	return ok
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int         //leader's term
	LeaderID     int         //so follower can redirect clients
	PrevLogIndex int         //index of log entry immediately preceding new ones
	PrevLogTerm  int         //term of prevLogIndex entry
	Entries      []LogRecord //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int         //leader's commitIndex
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm

	// Implement optimization
	ConflictTerm  int //term of conflicting entry
	ConflictIndex int //first index with conflicting term
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Lock raft
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteCount = 0
		rf.state = Follower
		rf.persist()
	}

	// If I am a candidate and leader's term is at least as large as mine, convert to follower
	if rf.state == Candidate && args.Term >= rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteCount = 0
		rf.persist()
	}

	// Set reply term to current term and the heartbeat flag
	reply.Term = rf.currentTerm
	rf.heartbeat = true

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		// Implement optimization
		// If prevLogIndex is greater than the index of the last entry in the receiver’s log, no conflict in logs
		if args.PrevLogIndex >= len(rf.log) {
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1
		} else { // Find the first index whose term doesn't match
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			for i := args.PrevLogIndex; i >= 0; i-- {
				if rf.log[i].Term != reply.ConflictTerm {
					reply.ConflictIndex = i + 1
					break
				}
			}
		}
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	for i := 0; i < len(args.Entries); i++ {
		if args.PrevLogIndex+i+1 < len(rf.log) {
			if rf.log[args.PrevLogIndex+i+1].Term != args.Entries[i].Term {
				rf.log = rf.log[:args.PrevLogIndex+i+1]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i])
			rf.persist()
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))

		// Apply log entries to state machine
		go rf.applyState()
	}

	// Set reply success to true
	reply.Success = true
}

// send an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.voteCount = 0
		rf.state = Follower
		rf.persist()
	}

	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		// Append command to log
		rf.log = append(rf.log, LogRecord{rf.currentTerm, command})
		rf.persist()
		index = len(rf.log) - 1
		term = rf.currentTerm
		isLeader = true

		// Update nextIndex and matchIndex
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me] = index
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Candidate code
func (rf *Raft) doCandidate() {
	// On conversion to candidate, start election:
	// Increment currentTerm
	rf.currentTerm++
	// Vote for self
	rf.votedFor = rf.me
	rf.persist()
	rf.voteCount = 1
	rf.mu.Unlock()

	// Send RequestVote RPCs to all other servers
	go rf.requestVotes()

	// Reset election timer: sleep for time between 750 and 1000 ms
	time.Sleep(time.Duration(rand.Intn(250)+750) * time.Millisecond)
}

// requestVotes sends RequestVote RPCs to all other servers
func (rf *Raft) requestVotes() {
	// Lock raft
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Create RequestVoteArgs
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	// Send RequestVote RPCs to all other servers concurrently
	for i := 0; i < len(rf.peers); i++ {
		// Stop if state is not candidate
		if rf.state != Candidate {
			return
		}

		if i != rf.me {
			go func(server int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, &args, &reply)

				// If votes received from majority of servers: become leader
				if ok && reply.VoteGranted {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// If AppendEntries RPC received from new leader: convert to follower
					// Check state
					if rf.state != Candidate {
						return
					}

					rf.voteCount++
					if rf.voteCount > len(rf.peers)/2 {
						rf.state = Leader
						// Initialize nextIndex and matchIndex for each server
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}
						rf.matchIndex[rf.me] = len(rf.log) - 1

						// On conversion to leader, start heartbeat
						go rf.sendHeartbeats(-1)
					}
				}
			}(i)
		}
	}
}

// Follower code
func (rf *Raft) doFollower() {
	// If election timeout elapses without receiving AppendEntries RPC from current leader
	// or granting vote to candidate: convert to candidate
	if rf.heartbeat {
		rf.heartbeat = false // Reset heartbeat
		rf.mu.Unlock()

		// Reset election timer: sleep for time between 1000 and 1500 ms
		time.Sleep(time.Duration(rand.Intn(500)+1000) * time.Millisecond)
	} else {
		// Convert to candidate
		rf.state = Candidate
		rf.mu.Unlock()
	}
}

// Leader code
func (rf *Raft) doLeader() {
	numPeers, me := len(rf.peers), rf.me
	rf.mu.Unlock()

	// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
	for i := 0; i < numPeers; i++ {
		if i != me {
			//Get necessary variables from rf
			rf.mu.Lock()
			// If no longer leader, return
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			if rf.state != Leader {
				rf.mu.Unlock()
				break
			}
			if len(rf.log)-1 >= rf.nextIndex[i] {
				rf.mu.Unlock()
				go rf.sendLogEntries(i)
			} else {
				rf.mu.Unlock()
				// Send heartbeats to all other servers
				go rf.sendHeartbeats(i)
			}
		}
	}

	// Send heartbeats 10 times per second
	time.Sleep(100 * time.Millisecond)
}

// sendLogEntries sends AppendEntries RPCs to a server
func (rf *Raft) sendLogEntries(server int) {
	// Lock raft
	rf.mu.Lock()

	// If nextIndex is out of range, return
	if rf.nextIndex[server] < 1 {
		rf.mu.Unlock()
		return
	}

	// If not leader anymore, return
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	// send AppendEntries RPC with log entries starting at nextIndex
	// Create AppendEntriesArgs
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
		Entries:      rf.log[rf.nextIndex[server]:],
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(server, &args, &reply)

	// If successful: update nextIndex and matchIndex for follower
	if ok {
		rf.mu.Lock()
		// If I am not the leader, return
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// If successful: update nextIndex and matchIndex for follower (§5.3)
		if reply.Success {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.nextIndex[server] = len(rf.log)
			rf.matchIndex[server] = len(rf.log) - 1

			// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
			// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
			for N := rf.commitIndex + 1; N < len(rf.log); N++ {
				count := 0
				for _, matchIndex := range rf.matchIndex {
					if matchIndex >= N {
						count++
					}
				}
				if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
					rf.commitIndex = N
					go rf.applyState()
				}
			}
		} else {
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
			rf.mu.Lock()
			// If conflictTerm is -1, then the follower's log is shorter than the leader's
			if reply.ConflictTerm == -1 {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				// If conflictTerm is not -1, then the follower's log is longer than the leader's
				// Find the first index in the follower's log with term equal to conflictTerm
				for i := len(rf.log) - 1; i >= 0; i-- {
					if rf.log[i].Term == reply.ConflictTerm {
						rf.nextIndex[server] = i + 1
						break
					}
				}
			}
			rf.mu.Unlock()
		}
	} else {
		// If AppendEntries fails because of RPC error: retry (§5.3)
		rf.mu.Lock()
		rf.nextIndex[server]--
		rf.mu.Unlock()
	}
}

// sendHeartbeats sends heartbeat AppendEntries RPCs to all other servers
func (rf *Raft) sendHeartbeats(singleServer int) {
	rf.mu.Lock()
	// Create AppendEntriesArgs
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: len(rf.log) - 1,
		PrevLogTerm:  rf.log[len(rf.log)-1].Term,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	// Send AppendEntries RPCs to all other servers concurrently or to a single server if specified
	if singleServer == -1 {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(server int) {
					var reply AppendEntriesReply
					ok := rf.sendAppendEntries(server, &args, &reply)

					// If reply not successful, decrement nextIndex and retry
					if ok && !reply.Success {
						rf.mu.Lock()
						// If not leader anymore, return
						if rf.state != Leader {
							rf.mu.Unlock()
							return
						}
						// If conflictTerm is -1, then the follower's log is shorter than the leader's
						if reply.ConflictTerm == -1 {
							rf.nextIndex[server] = reply.ConflictIndex
						} else {
							// If conflictTerm is not -1, then the follower's log is longer than the leader's
							// Find the first index in the follower's log with term equal to conflictTerm
							for i := len(rf.log) - 1; i >= 0; i-- {
								if rf.log[i].Term == reply.ConflictTerm {
									rf.nextIndex[server] = i + 1
									break
								}
							}
						}
						rf.mu.Unlock()
					}
				}(i)
			}
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(singleServer, &args, &reply)

		// If reply not successful, decrement nextIndex and retry
		if ok && !reply.Success {
			rf.mu.Lock()
			// If conflictTerm is -1, then the follower's log is shorter than the leader's
			if reply.ConflictTerm == -1 {
				rf.nextIndex[singleServer] = reply.ConflictIndex
			} else {
				// If conflictTerm is not -1, then the follower's log is longer than the leader's
				// Find the first index in the follower's log with term equal to conflictTerm
				for i := len(rf.log) - 1; i >= 0; i-- {
					if rf.log[i].Term == reply.ConflictTerm {
						rf.nextIndex[singleServer] = i + 1
						break
					}
				}
			}
			rf.mu.Unlock()
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// Lock raft
		rf.mu.Lock()

		if rf.state == Leader {
			rf.doLeader()
		} else if rf.state == Candidate {
			rf.doCandidate()
		} else if rf.state == Follower {
			rf.doFollower()
		}
	}
}

// Function that applies commands to state machine
func (rf *Raft) applyState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{true, rf.log[i].Command, i}
		}
		rf.lastApplied = rf.commitIndex
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
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

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.log = make([]LogRecord, 1)
	rf.state = Follower
	rf.heartbeat = false
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// stagger the starting times of the ticker goroutines to avoid simultaneous elections
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(300)))
	go rf.ticker()

	return rf
}
