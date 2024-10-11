package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	eleTimeout    = 300 * time.Millisecond
	heartBeatTime = time.Second / 8
)

// 每个logentry都有Term
type logEntry struct {
	Command interface{}
	Term    int
}

type Role int

const (
	Follower Role = iota
	Leader
	Candidate
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
	role Role

	lastUpdateTime time.Time

	currentTerm int
	votedFor    int
	logs        []logEntry
	commitIndex int
	lastApplied int
	applyChan   chan ApplyMsg

	// for leader
	nextIndex  []int
	matchIndex []int

	// for snap
	hasCompressLen int
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var Term int
	var isleader bool
	Term, isleader = rf.currentTerm, rf.role == Leader
	return Term, isleader
}

func (rf *Raft) checkIdenty(role Role) bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	check := (rf.role == role)
	return check
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term              int
	LeaderId          int
	PreLogIndex       int
	PreLogTerm        int
	Entries           []logEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	// Your data here (3A, 3B).
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) isLogUpToDate(lastLogTerm, lastLogIndex int) bool {
	// 比当前节点更新 return true
	if rf.logs[len(rf.logs)-1].Term < lastLogTerm || rf.logs[len(rf.logs)-1].Term == lastLogTerm && len(rf.logs)-1 <= lastLogIndex {
		return true
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println(rf.me, "收到投票请求 myterm:", rf.currentTerm, " args term:", args.Term, " votefor:", rf.votedFor)

	// 如果请求中的Term比当前Term小，直接拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}

	// 如果Term>=相同，检查是否已经投票以及日志是否更新
	if rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {

		// 投票给候选者
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.lastUpdateTime = time.Now() // 重置选举超时
	} else {
		// 候选者日志不如自己新，拒绝投票
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.votedFor = -1
		return
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		if rf.role == Leader {
			rf.role = Follower
		}
		rf.votedFor = -1
	}
	rf.lastUpdateTime = time.Now()
	reply.Term = rf.currentTerm

	if rf.getPrelogIndex() < args.PreLogIndex || rf.getPrelogTerm() != args.PreLogTerm {
		reply.Success = false

		// 获取冲突日志的任期
		if rf.getPrelogIndex() < args.PreLogIndex {
			// 如果 PreLogIndex 比我们当前的日志索引大，返回当前日志的长度
			reply.ConflictTerm = -1
			reply.ConflictIndex = len(rf.logs)
		} else {
			// 返回冲突的任期
			reply.ConflictTerm = rf.logs[args.PreLogIndex].Term
			// 查找该冲突任期的第一个日志条目索引
			conflictIdx := 0
			for j := args.PreLogIndex - 1; j >= 0; j-- {
				if rf.logs[j].Term != reply.ConflictTerm {
					conflictIdx = j + 1
					break
				}
			}

			reply.ConflictIndex = conflictIdx

		}

		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different Terms),
	// delete the existing entry and all that follow it (§5.3)
	if len(rf.logs) > args.PreLogIndex+1 && rf.logs[args.PreLogIndex+1].Term != args.Term {
		rf.logs = rf.logs[:args.PreLogIndex+1]
	}

	// 4. Append any new entries not already in the log
	for i, entry := range args.Entries {
		if args.PreLogIndex+1+i >= len(rf.logs) {
			rf.logs = append(rf.logs, entry)
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
		rf.applyEntries() // 应用新提交的日志条目到状态机
	}

	reply.Success = true

	return
}

// 应用已提交的日志条目到状态机
func (rf *Raft) applyEntries() {
	// 例如：遍历日志中已提交但尚未应用的条目，将其发送给状态机
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyChan <- applyMsg
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
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(Command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}
	newLogEntry := logEntry{
		Command: Command,
		Term:    rf.currentTerm,
	}
	rf.logs = append(rf.logs, newLogEntry)
	index := len(rf.logs) - 1
	Term := rf.currentTerm

	// Your code here (3B).

	return index, Term, true
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

func (rf *Raft) getPrelogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getPrelogTerm() int {
	if len(rf.logs) > 0 {
		return rf.logs[len(rf.logs)-1].Term
	} else {
		return -1
	}
}

func (rf *Raft) genAppendEntriesArgs(serverID int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 确定 PreLogIndex 和 PreLogTerm
	preLogIndex := rf.nextIndex[serverID] - 1
	preLogTerm := -1
	if preLogIndex >= 0 {
		preLogTerm = rf.logs[preLogIndex].Term
	}

	args := &AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		PreLogIndex:       preLogIndex,
		PreLogTerm:        preLogTerm,
		LeaderCommitIndex: rf.commitIndex,
		Entries:           rf.logs[rf.nextIndex[serverID]:],
	}
	return args
}

func (rf *Raft) handleAppendReply(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(serverId, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 1. term 更新
		// 2. term正常但日志不一致
		if reply.Term > rf.currentTerm {
			rf.role = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			return
		}
		// 如果追加日志失败，说明日志冲突
		if !reply.Success {
			// 如果跟随者返回了 ConflictTerm 和 ConflictIndex
			if reply.ConflictTerm != -1 {
				// 查找Leader日志中 ConflictTerm 的索引
				conflictIdx := -1
				for j := len(rf.logs) - 1; j >= 0; j-- {
					if rf.logs[j].Term == reply.ConflictTerm {
						conflictIdx = j
						break
					}
				}

				// 如果 Leader 没有 ConflictTerm，直接设置 nextIndex 为 ConflictIndex
				if conflictIdx == -1 {
					rf.nextIndex[serverId] = reply.ConflictIndex
				} else {
					// 否则，设置 nextIndex 为 Leader 中 ConflictTerm 的最后一个日志的索引+1
					rf.nextIndex[serverId] = conflictIdx + 1
				}
			} else {
				// 表明follower日志少
				rf.nextIndex[serverId] = reply.ConflictIndex
			}
			return
		} else {
			// 如果追加日志成功，更新 matchIndex 和 nextIndex
			rf.matchIndex[serverId] = args.PreLogIndex + len(args.Entries)
			rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1
		}

		// 检测是否有可提交的log
		n := len(rf.peers)

		for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
			count := 1

			for j := 0; j < n; j++ {
				if rf.matchIndex[j] >= i {
					count++
					if count > n/2 {
						rf.commitIndex = i
						go rf.applyEntries()
						break
					}
				}
			}
		}

	}
}

func (rf *Raft) heartBeat() {
	// 心跳
	for !rf.killed() && rf.checkIdenty(Leader) {

		for i := 0; i < len(rf.peers) && rf.checkIdenty(Leader); i++ {
			if i != rf.me {
				args := rf.genAppendEntriesArgs(i)
				reply := &AppendEntriesReply{}

				go rf.handleAppendReply(i, args, reply)
			}
		}
		time.Sleep(heartBeatTime)

	}
}

// todo
func (rf *Raft) convertToLeader() {
	fmt.Println(rf.me, "成为leader")
	rf.role = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
	go rf.heartBeat()

}

func (rf *Raft) startElection() {
	// candidateId  int
	// lastLogIndex int
	// lastLogTerm  int
	rf.mu.Lock()
	receivedNum := 1
	rf.role = Candidate

	rf.currentTerm += 1

	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	rf.mu.Unlock()
	fmt.Println(rf.me, "开始选举,term", rf.currentTerm)

	// 使用原子变量或锁来保护 receivedNum

	// 启动 goroutine 发送请求
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peerIndex int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(peerIndex, args, reply) // 使用 peerIndex 而不是 i
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.role != Candidate {
					return
				}

				if ok && reply.VoteGranted {
					fmt.Println(rf.me, "收到1票")
					receivedNum++
				}
				if receivedNum > len(rf.peers)/2 {
					rf.convertToLeader()
					return
				}

				if ok && !reply.VoteGranted && reply.Term > rf.currentTerm {
					rf.role = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
				}
			}(i)
		}
	}

}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		timeout := (rand.Int63() % 300)

		rf.mu.Lock()
		lastTime := rf.lastUpdateTime
		rf.mu.Unlock()
		if !rf.checkIdenty(Leader) && time.Since(lastTime) > eleTimeout+time.Duration(timeout)*time.Millisecond {
			rf.startElection()
		}

		time.Sleep(time.Duration(timeout) * time.Millisecond)
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
	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// Initialize the basic state
	rf.currentTerm = 0                           // 初始时任期为0
	rf.votedFor = -1                             // 初始时没有投票对象
	rf.logs = append(rf.logs, logEntry{Term: 0}) // 初始化一个空的日志，索引为0，任期为0
	rf.commitIndex = 0                           // 已提交的日志索引
	rf.lastApplied = 0                           // 已应用的日志索引

	// Leader 特有的状态初始化（先初始化为0，成为 Leader 后再更新）
	rf.nextIndex = make([]int, len(peers))  // Leader 对每个节点的 nextIndex
	rf.matchIndex = make([]int, len(peers)) // Leader 对每个节点的 matchIndex

	// 初始化角色为 Follower，定期检查超时进行选举
	rf.role = Follower
	rf.lastUpdateTime = time.Now() // 记录最后一次收到心跳的时间

	// Snapshot 数据压缩初始化（假设未进行任何压缩）
	rf.hasCompressLen = 0

	// 初始化 apply channel，用于发送已提交的日志条目给状态机
	rf.applyChan = applyCh

	// initialize from state persisted before a crash (恢复持久化状态)
	// rf.readPersist(persister.ReadRaftState())

	// 启动心跳和选举超时的 ticker goroutine
	fmt.Println("总共节点数：", len(rf.peers))
	rand.Seed(time.Now().UnixNano())
	go rf.ticker()

	return rf

}
