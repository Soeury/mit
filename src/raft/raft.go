package raft

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"mit/labgob"
	"mit/labrpc"
	log "mit/log"
)

// 当每个 Raft 节点感知到连续的日志条目被提交时，
// 该节点应通过 Make() 时传入的 applyCh 通道，
// 向同一服务器上的服务（或测试器）发送 ApplyMsg。
// 设置 CommandValid 为 true 表示该 ApplyMsg 包含
// 一个新提交的日志条目。
//
// 在实验 2D 部分，您需要通过 applyCh 发送其他类型的消息
// （如快照），但对于其他用途的消息，
// 需要将 CommandValid 设置为 false
//
// 快照：日志同步后进行压缩 -> 执行快照
type ApplyMsg struct {
	CommandValid bool // 日志包含
	Command      any  // 日志内容
	CommandIndex int  // 日志索引

	SnapshotValid bool   // 是否启用快照
	Snapshot      []byte // 快照数据
	SnapshotTerm  int    // 快照最后一个日志任期
	SnapshotIndex int    // 快照最后一个日志索引
}

type Log struct {
	Term    int // 日志所属任期
	Command any // 日志内容
}

type Raft struct {
	mu        sync.Mutex          // 并发锁
	peers     []*labrpc.ClientEnd // 所有节点的 RPC 终端
	persister *Persister          // 持久化存储 [persister.go]
	me        int                 // 本节点在 peers 中的索引
	dead      int32               // 由 Kill 设置的终止标志

	term     int   // 当前任期
	votedFor int   // 当前任期投票给的 candidate ID
	log      []Log // 预设日志数组

	nextIndex  []int // leader 下的每个 follower 的下一个待同步的日志索引
	matchIndex []int // leader 下的每个 follower 和 Leader 一致的日志最大索引

	voteTimer  *time.Timer
	heartTimer *time.Timer
	character  int        // 节点角色
	rands      *rand.Rand // 随机数种

	applyCh     chan ApplyMsg // 日志应用到状态机的通道
	commitIndex int           // 可以提交的最高日志索引
	lastApplied int           // 已应用的最高日志索引

	condApply *sync.Cond // ?

	snapshot         []byte // 快照数据
	lastIncludeIndex int    // 快照最高日志索引
	lastIncludeTerm  int    // 快照最高日志任期
}

const (
	leader = iota
	candidate
	follower
)

const (
	HeartBeatTimeOut = 101 // 心跳超时
	ElectTimeOutBase = 450 // 选举超时
)

type RequestVoteRequest struct {
	Term         int // candidate 当前任期
	CandidateID  int // candidate ID: 节点的 me 值
	LastLogIndex int // candidate 最后日志的索引
	LastLogTerm  int // candidate 最后日志的任期
}

type RequestVoteResponse struct {
	Term        int  // 接收者当前任期
	VoteGranted bool // 是否同意投票
}

type AppendEntriesRequest struct {
	Term         int   // leader 当前任期
	LeaderID     int   // leader ID: 节点的 me 值
	PrevLogIndex int   // leader 需要同步日志的前一个日志索引
	PreLogTerm   int   // PrevLogIndex 的任期
	CommitIndex  int   // leader 已提交的最高日志索引
	Entries      []Log // 给 follower 追加的日志
}

type AppendEntriesResponse struct {
	Term    int  // 接收者当前任期
	Success bool // 接收者是否同步成功
	XTerm   int  // follower中与Leader冲突的log对应的term
	XIndex  int  // follower 中，任期为xterm的第一条日志索引
	XLen    int  // follower中log的长度
}

type InstallSnapshotRequest struct {
	Term             int    // leader 当前任期
	LeaderID         int    // leader ID: 节点的 me 值
	LastIncludeIndex int    // 快照包含的最后一个日志的索引
	LastIncludeTerm  int    // 最后一个日志的任期
	Data             []byte // 快照原始字节数据
	LastIncludeCmd   any    // 用于idx=0出日志占位
}

type InstallSnapshotResponse struct {
	Term int // 接收者的任期
}

// AppendEntries 被SendAppendEntries()函数包装，对外使用SendAppendEntries进行投票
func (rf *Raft) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	if req.Term < rf.term {
		resp.Term = rf.term
		resp.Success = false
		log.Printf("server %d received old leader msg\n", rf.me)
		return
	}

	rf.ResetVoteTimer() // 收到新leader的消息，重置定时器

	if req.Term > rf.term {
		rf.term = req.Term
		rf.votedFor = -1
		rf.character = follower
		rf.Persist()
	} // 更新当前节点状态

	if len(req.Entries) == 0 {
		// hear beat
		log.Printf("server %d receives heart beat from leader %d, term is %d\n", rf.me, req.LeaderID, rf.term)
	} else {
		// logs
		log.Printf("server %d receives logs from leader %d, term is %d\n", rf.me, req.LeaderID, rf.term)
	}

	isConflict := false // 用于记录是否存在冲突

	// 校验 PreLogIndex 和 PreLogTerm，检查冲突
	if req.PrevLogIndex < rf.lastIncludeIndex {
		// rpc 过时
		resp.Success = true
		resp.Term = rf.term
		return
	} else if req.PrevLogIndex >= rf.VirtualLogIndex(len(rf.log)) {
		// preLogIndex 这个位置没有日志，存在冲突
		resp.XTerm = -1 // -1 表示该位置日志不存在
		resp.XLen = rf.VirtualLogIndex(len(rf.log))
		isConflict = true
		log.Printf("server %d loss logs before PreLogIndex\n", rf.me)
	} else if rf.log[rf.RealLogIndex(req.PrevLogIndex)].Term != req.PreLogTerm {
		// preLogIndex 日志存在，但是term不匹配，存在冲突
		// 找到这个不匹配任期的日志的第一个index，并记录
		resp.XTerm = rf.log[rf.RealLogIndex(req.PrevLogIndex)].Term
		i := req.PrevLogIndex
		for i > rf.commitIndex && rf.log[rf.RealLogIndex(i)].Term == resp.XTerm {
			i -= 1
		}

		resp.XIndex = i + 1                         // preLogIndex位置上冲突任期的第一条日志的位置
		resp.XLen = rf.VirtualLogIndex(len(rf.log)) // 包括快照部分长度
		isConflict = true
		log.Printf("server %d log term in PreLogIndex is error, log term is %d , must be %d", rf.me, resp.XTerm, req.PreLogTerm)
	}

	if isConflict {
		resp.Term = rf.term
		resp.Success = false
		return
	} // 冲突存在直接返回

	// 没有冲突: 追加or覆盖日志
	// ridx > len(rf.log) 说明preLogIndex位置没有日志，会被设置为冲突
	for idx, logg := range req.Entries {
		ridx := rf.RealLogIndex(req.PrevLogIndex) + 1 + idx
		if ridx < len(rf.log) && rf.log[ridx].Term != logg.Term {
			rf.log = rf.log[:ridx]
			rf.log = append(rf.log, req.Entries[idx:]...)
			break
		} else if ridx == len(rf.log) {
			rf.log = append(rf.log, req.Entries[idx:]...)
			break
		}
	}

	if len(req.Entries) != 0 {
		log.Printf("server %d 【append logs success】, last applied: %d , len(log):%d\n", rf.me, rf.lastApplied, len(rf.log))
		log.Solidf("server %d after append logs: %+v\n", rf.me, rf.log)
	}
	rf.Persist()

	resp.Success = true
	resp.Term = rf.term

	// 更新 commitIndex
	if req.CommitIndex > rf.commitIndex {
		before := rf.commitIndex

		if req.CommitIndex > rf.VirtualLogIndex(len(rf.log)-1) {
			rf.commitIndex = rf.VirtualLogIndex(len(rf.log) - 1)
		} else {
			rf.commitIndex = req.CommitIndex
		}
		log.Printf("server %d start check commit, comminIndex[before:after][%d:%d]\n", rf.me, before, rf.commitIndex)
		rf.condApply.Signal() // commitIndex更新, 解除CommitChecker中的condApply的阻塞
	}
}

// RequestVote 被SendRequestVote()函数包装，对外使用SendRequestVote进行投票
func (rf *Raft) RequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) {
	rf.mu.Lock()

	defer func() {
		rf.mu.Unlock()
	}()

	if req.Term < rf.term {
		// candidate任期落后,状态转变在GetVoteAnswer中处理
		resp.Term = rf.term
		log.Printf("server %d refuse vote for server %d, because of old term\n", rf.me, req.CandidateID)
		return
	} else if req.Term > rf.term {
		// 当前节点投票作废
		rf.term = req.Term
		rf.votedFor = -1
		rf.character = follower
		rf.Persist()
	}

	// 没投过票则检查投票的条件是否满足
	if rf.votedFor == -1 || rf.votedFor == req.CandidateID {
		if req.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(req.LastLogTerm == rf.log[len(rf.log)-1].Term && req.LastLogIndex >= rf.VirtualLogIndex(len(rf.log)-1)) {
			rf.term = req.Term
			rf.votedFor = req.CandidateID
			rf.character = follower

			rf.ResetVoteTimer()
			rf.Persist()

			resp.Term = rf.term
			resp.VoteGranted = true
			log.Printf("server %d vote for candidate %d\n", rf.me, req.CandidateID)
			return
		} else {
			// 拒绝投票, 日志不是最新
			if req.LastLogTerm < rf.log[len(rf.log)-1].Term {
				log.Printf("server %d refuse vote for server %d, because of old log term\n", rf.me, req.CandidateID)
			} else {
				log.Printf("server %d refuse vote for server %d, because of old logs\n", rf.me, req.CandidateID)
			}
		}
	} else {
		// 当前节点当前任期已投过票给其他节点
		log.Printf("server %d has voted for other candidate yet\n", rf.me)
	}

	resp.Term = rf.term
	resp.VoteGranted = false
}

// InstallSnapshot 被SendInstallSnap()函数包装，对外使用SendInstallSnap对其他节点安装快照
func (rf *Raft) InstallSnapshot(req *InstallSnapshotRequest, resp *InstallSnapshotResponse) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 接收到过期term的lealder传来的快照, 拒绝
	if req.Term < rf.term {
		resp.Term = rf.term
		log.Printf("server %d received a lower term Snapshot from server %d, old term:%d, new term:%d\n", rf.me, req.LeaderID, req.Term, rf.term)
		return
	}

	// 接收到更高任期leader传来的快照, 修改状态
	if req.Term > rf.term {
		rf.term = req.Term
		rf.votedFor = -1
		log.Printf("server %d received a bigger term Snapshot from server %d\n", rf.me, req.LeaderID)
	}

	rf.character = follower
	rf.ResetVoteTimer() // 收到leader消息，重置定时器，必须要执行的，不管任期
	resp.Term = rf.term
	if req.LastIncludeIndex < rf.lastIncludeIndex || req.LastIncludeIndex < rf.commitIndex {
		// 旧快照, 无视
		return
	}

	// 判断接收快照后该节点后是否还有日志
	hasEntry := false
	Index := 0
	for ; Index < len(rf.log); Index++ {
		if rf.VirtualLogIndex(Index) == req.LastIncludeIndex && rf.log[Index].Term == req.LastIncludeTerm {
			hasEntry = true
			break
		}
	}

	// 截取日志
	if hasEntry {
		log.Printf("server %d install snapshot, save logs after lastIncludeIndex:%d\n", rf.me, req.LastIncludeIndex)
		rf.log = rf.log[Index:] // 保留一个日志在index=0处占位
	} else {
		log.Printf("server %d install snapshot, save no logs after lastIncludeIndex\n", rf.me)
		rf.log = make([]Log, 0) // index=0 处的日志用来占位
		rf.log = append(rf.log, Log{Term: rf.lastIncludeTerm, Command: req.LastIncludeCmd})
	}

	// 更新节点信息
	rf.snapshot = req.Data
	rf.lastIncludeIndex = req.LastIncludeIndex
	rf.lastIncludeTerm = req.LastIncludeTerm

	if rf.commitIndex < req.LastIncludeIndex {
		rf.commitIndex = rf.lastIncludeIndex
	}

	if rf.lastApplied < req.LastIncludeIndex {
		rf.lastApplied = rf.lastIncludeIndex
	}

	// 应用快照数据到状态机
	msg := &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotTerm:  req.LastIncludeTerm,
		SnapshotIndex: req.LastIncludeIndex,
	}
	rf.applyCh <- *msg
	rf.Persist()
}

// SendAppendEntries() 函数用于HandleAppendEntried()中，获取RPC调用结果与resp
func (rf *Raft) SendAppendEntries(server int, req *AppendEntriesRequest, resp *AppendEntriesResponse) bool {
	return rf.peers[server].Call("Raft.AppendEntries", req, resp)
}

// SendRequestVote 用于GetVoteAnswer()函数使用，返回投票结果
func (rf *Raft) SendRequestVote(server int, req *RequestVoteRequest, resp *RequestVoteResponse) bool {
	return rf.peers[server].Call("Raft.RequestVote", req, resp)
}

func (rf *Raft) SendInstallSnap(server int, req *InstallSnapshotRequest, resp *InstallSnapshotResponse) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", req, resp)
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// GetRandomElectTimeOut 设置随机选举超时
func GetRandomElectTimeOut(rd *rand.Rand) int {
	plusMs := int(rd.Float64() * 150) // 0 - 150 ms
	return plusMs + ElectTimeOutBase
}

// ResetVoteTimer 重置投票定时器
func (rf *Raft) ResetVoteTimer() {
	randsTimeOut := GetRandomElectTimeOut(rf.rands)
	rf.voteTimer.Reset(time.Duration(randsTimeOut) * time.Millisecond)
}

// ResetHeartTimer 重置心跳定时器
func (rf *Raft) ResetHeartTimer(timeStamp int) {
	rf.heartTimer.Reset(time.Duration(timeStamp) * time.Millisecond)
}

// RealLogIndex 获取真实的索引
func (rf *Raft) RealLogIndex(vIndex int) int {
	return vIndex - rf.lastIncludeIndex
}

// VirtualLogIndex 获取全局增长的索引
func (rf *Raft) VirtualLogIndex(rIndex int) int {
	return rIndex + rf.lastIncludeIndex
}

// Snapshot 对节点进行快照, 更新节点快照, 截断日志
//
// Snapshot() 函数用于config文件
func (rf *Raft) Snapshot(index int, snapshot []byte) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < index || index <= rf.lastIncludeIndex {
		log.Printf("server %d refused Snapshot, index=%d, commitIndex=%d, lastIncludeIndex=%d\n", rf.me, index, rf.commitIndex, rf.lastIncludeIndex)
		return
	}

	log.Printf("server %d received Snapshot, index=%d, commitIndex=%d, lastIncludeIndex=%d\n", rf.me, index, rf.commitIndex, rf.lastIncludeIndex)
	rf.snapshot = snapshot

	// 更新节点快照信息, 截断日志
	rf.lastIncludeTerm = rf.log[rf.RealLogIndex(index)].Term
	rf.log = rf.log[rf.RealLogIndex(index):] // 索引为0处保留一个日志
	rf.lastIncludeIndex = index

	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	rf.Persist()
}

// HandleInstallSnapShot leader向其他节点处理快照
//
// HandleInstallSnapShot()函数用于在SendHearBeats()中发送快照
func (rf *Raft) HandleInstallSnapShot(peer int) {

	resp := &InstallSnapshotResponse{}
	rf.mu.Lock()

	// leader状态被修改, 不再是leader, 直接返回
	if rf.character != leader {
		rf.mu.Unlock()
		return
	}

	req := &InstallSnapshotRequest{
		Term:             rf.term,
		LeaderID:         rf.me,
		LastIncludeIndex: rf.lastIncludeIndex,
		LastIncludeTerm:  rf.lastIncludeTerm,
		Data:             rf.snapshot,
		LastIncludeCmd:   rf.log[0].Command, // 用于index=0时log占位
	}
	rf.mu.Unlock()

	// ***向peer发送快照RPC时不能持有锁
	ok := rf.SendInstallSnap(peer, req, resp)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	if rf.character != leader || rf.term != req.Term {
		// leader状态被修改, 不能进行后面的操作
		return
	}

	if resp.Term > rf.term {
		// leader发现自己的term已过期, 修改状态, 不能进行后面的操作
		rf.term = resp.Term
		rf.character = follower
		rf.votedFor = -1
		rf.ResetVoteTimer()
		rf.Persist()
		return
	}

	// 发送快照后, 当前leader正常
	// 更新 matchIndex 和 nextIndex
	if rf.matchIndex[peer] < req.LastIncludeIndex {
		rf.matchIndex[peer] = req.LastIncludeIndex
	}
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1
}

// HandleAppendEntried leader向其他节点处理日志
//
// HandleAppendEntried()函数用于在SendHearBeats()发送日志, 由leader调用
func (rf *Raft) HandleAppendEntried(peer int, req *AppendEntriesRequest) {

	// 利用心跳重新发送日志, 这里不处理
	resp := &AppendEntriesResponse{}
	ok := rf.SendAppendEntries(peer, req, resp)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	if rf.character != leader || req.Term != rf.term {
		// 节点同步日志时状态被更改，不安全，直接返回
		return
	}

	// 1. 同步成功, 修改并计算 MatchIndex and NextIndex
	if resp.Success {
		newMatchIndex := req.PrevLogIndex + len(req.Entries)
		if newMatchIndex > rf.matchIndex[peer] {
			// 期间收到快照
			rf.matchIndex[peer] = newMatchIndex
		}

		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		lastIndex := rf.VirtualLogIndex(len(rf.log) - 1) // ? ? ? ? ?有时候-1有时候不-1

		log.Printf("leader %d start update CommitIndex=%d, LastIncludeIndex=%d\n", rf.me, rf.commitIndex, rf.lastIncludeIndex)

		// lastIndex 当前 leader 的最大日志索引
		// ***计算大多数节点已同步的最大日志索引，并且该日志必须是这个leader任期内的日志
		// ***计算出来的最大日志索引之前的日志就都可以提交
		for lastIndex > rf.commitIndex {
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= lastIndex && rf.log[rf.RealLogIndex(lastIndex)].Term == rf.term {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				break
			}
			lastIndex -= 1
		}

		rf.commitIndex = lastIndex
		rf.condApply.Signal() // commitIndex更新, 解除CommitChecker中的condApply的阻塞
		return
	}

	// 2. 收到更新的任期resp, 更新状态
	if resp.Term > rf.term {
		log.Printf("server %d (old leader term:%d) received a newer term %d resp from server %d\n", rf.me, req.Term, resp.Term, peer)
		rf.term = resp.Term
		rf.character = follower
		rf.votedFor = -1
		rf.ResetVoteTimer()
		rf.Persist()
		return
	}

	// 3. PreLogIndex 位置的日志不匹配问题
	if resp.Term == rf.term && rf.character == leader {

		// PreLogIndex 位置不存在日志
		if resp.XTerm == -1 {
			log.Printf("leader %d receives server's %d resp, becase log in preLogIndx is null, before nextIndex:%d , after nextIndex:%d\n", rf.me, peer, rf.nextIndex[peer], resp.XIndex)
			if rf.lastIncludeIndex >= resp.XLen {
				// 日志被截断，直接发送快照
				rf.nextIndex[peer] = rf.lastIncludeIndex
			} else {
				rf.nextIndex[peer] = resp.XLen
			}
			return
		}

		// PreLogIndex 位置的日志 term 不匹配
		// PreLogIndex = rf.nextIndex[peer] - 1
		//   -- XTerm: peer节点上PreLogIndex位置上的日志的任期
		//   -- XIndex: peer节点上XTerm任期的第一条日志的位置
		//   -- XLen: peer节点日志的长度(包括快照)
		i := rf.nextIndex[peer] - 1
		if i < rf.lastIncludeIndex {
			i = rf.lastIncludeIndex
		}
		for i > rf.lastIncludeIndex && rf.log[rf.RealLogIndex(i)].Term > resp.XTerm {
			i -= 1
		}

		if i == rf.lastIncludeIndex && rf.log[rf.RealLogIndex(i)].Term > resp.XTerm {
			// 这里表示没有peer节点想要的日志，直接发送快照
			rf.nextIndex[peer] = rf.lastIncludeIndex
		} else if rf.log[rf.RealLogIndex(i)].Term == resp.XTerm {
			// 这里是默认peer在XTerm任期内的日志全都有吗?
			rf.nextIndex[peer] = i + 1
		} else {
			if resp.XIndex <= rf.lastIncludeIndex {
				rf.nextIndex[peer] = rf.lastIncludeIndex
			} else {
				rf.nextIndex[peer] = resp.XIndex
			}
		}

		return
	}
}

// GetVoteAnswer 给candidate调用的发起投票请求的函数
// 这里主要做发起投票请求并接收投票响应
//
// GetVoteAnswer() 函数用于收集投票结果并发送给CollectVote()函数
func (rf *Raft) GetVoteAnswer(peer int, req *RequestVoteRequest) bool {

	sendReq := *req
	reply := RequestVoteResponse{}
	ok := rf.SendRequestVote(peer, &sendReq, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	// 向节点发送请求期间candidate状态被修改! ! !
	if rf.character != candidate || sendReq.Term != rf.term {
		return false
	}

	if reply.Term > rf.term {
		// 说明当前节点term已经是旧的, 修改状态数据并保存
		rf.term = reply.Term
		rf.votedFor = -1
		rf.character = follower
		rf.Persist()
		// 状态落后则投票一定会被拒绝，这里不用返回
	}

	return reply.VoteGranted
}

// CollectVote candidate针对指定节点收集投票, 给candidate调用
// 这里作为所有投票请求结果的汇总中心, 注意两个锁
//
// CollectVote()函数用于candidate收集投票请求并处理结果, 作为汇总中心
func (rf *Raft) CollectVote(peer int, req *RequestVoteRequest, muVote *sync.Mutex, voteCount *int) {
	voteAnswer := rf.GetVoteAnswer(peer, req)
	if !voteAnswer {
		return
	}

	// 针对voteCount计算与candidate状态转换的锁
	// 保护共享变量 voteCount 的并发访问
	muVote.Lock()

	// 1. 超过大部分投票说明当前candidate已经转换为了leader,不用继续了
	// 获取大部分投票则直接返回
	// 防止多个collectVote协程同时对一个candidate进行leader转换
	// 确保针对一个candidate只进行一次leader转换
	if *voteCount > len(rf.peers)/2 {
		muVote.Unlock()
		return
	}

	// 2. 如果当前投票能构成大部分的投票，candidate开始转换为leader
	*voteCount += 1
	if *voteCount > len(rf.peers)/2 {
		rf.mu.Lock() // 这是当前协程单独持有的锁, 不持有Election的锁

		// 当前candidate收到过更新的term而更改了term,自身状态变更
		if rf.character != candidate || rf.term != req.Term {
			rf.mu.Unlock()
			muVote.Unlock()
			return
		}

		log.Printf("server %d 【become leader】, term is %d\n", rf.me, rf.term)
		rf.character = leader

		// 状态变更后重新初始化nextIndex和matchIndex
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = rf.VirtualLogIndex(len(rf.log))
			rf.matchIndex[i] = rf.lastIncludeIndex // 与所有节点已匹配的日志索引，从快照开始
		}
		rf.mu.Unlock()

		// 候选人成功当选leader后，发送心跳and同步日志or快照
		go rf.SendHearBeats()
	}

	muVote.Unlock()
}

// Election candidate 发起选举
// ***这里特别注意两个锁, 需要解释为什么用两个锁
//
// Election() 函数用在Ticker()中，Ticker超时之后用于发起选举
func (rf *Raft) Election() {

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	rf.term += 1 // 超时之后调用该函数时更新任期
	rf.character = candidate
	rf.votedFor = rf.me
	rf.Persist()

	voteCount := 1 // 自己的一票
	var muVote sync.Mutex

	log.Printf("server %d 【start request vote】, term is %d\n", rf.me, rf.term)
	req := &RequestVoteRequest{
		Term:         rf.term,
		CandidateID:  rf.me,
		LastLogIndex: rf.VirtualLogIndex(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	} // 本候选人的最新一条日志及任期

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.CollectVote(i, req, &muVote, &voteCount)
	} // rf.mu 锁在 goroutine 内部不持有
}

// CommitChecker 检查是否有提交
//
// CommitChecker() 函数用于在Make()函数中作为每个节点的后台
func (rf *Raft) CommitChecker() {

	log.Printf("server %d start Commit Checker!\n", rf.me)
	for !rf.Killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait() // 当前节点阻塞, 节点commitIndex更新后会解除阻塞
		}

		// msgBuf收集快照之后, commitIndex之前的所有日志
		msgBuf := make([]*ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		tempApplied := rf.lastApplied
		for rf.commitIndex > tempApplied {
			tempApplied += 1
			if tempApplied <= rf.lastIncludeIndex {
				continue // tempApplied 已经被快照截断，不用在发送了
			}

			if rf.RealLogIndex(tempApplied) >= len(rf.log) {
				log.Printf("server %d in commit checker log is out of index\n", rf.me)
			}

			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.RealLogIndex(tempApplied)].Command,
				CommandIndex: tempApplied,
				SnapshotTerm: rf.log[rf.RealLogIndex(tempApplied)].Term,
			} // SnapshotTerm 不发送可以吗? ? ? ? ?

			msgBuf = append(msgBuf, msg)
		}
		rf.mu.Unlock()

		// 解锁后可能出现快照
		for _, msg := range msgBuf {
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()

			rf.applyCh <- *msg

			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}

			// 这里会更新lastApplied索引
			// 中途没有快照会一直保持lastApplied + 1 = 当前msg索引
			//   -- 没有快照就在这个循环中应用日志
			//   -- 有快照则直接跳过，快照会提交日志
			rf.lastApplied = msg.CommandIndex
			rf.mu.Unlock()
		}
	}
}

// SendHearBeats leader向其他节点发送"心跳"
// (1.快照 2.日志 3.心跳) 都做为重置心跳计时器的"心跳"
// * 发送心跳前检查日志，pre日志在快照前则发送快照
// * pre日志在快照后则检查是否有新的日志，有新的日志则发送新的日志
// * 没有新的日志则发送心跳
//
// SendHearBeats() 函数由leader调用，在candidate当选leader后用于同步日志和发送心跳
func (rf *Raft) SendHearBeats() {
	for !rf.Killed() {
		<-rf.heartTimer.C // ? ? ? ? 这里不会阻塞吗? 如何同步
		rf.mu.Lock()

		if rf.character != leader {
			rf.mu.Unlock()
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			req := &AppendEntriesRequest{
				Term:         rf.term,
				LeaderID:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				CommitIndex:  rf.commitIndex,
			}

			sendInstallSnapShot := false

			// 这里检查是否需要发送快照, 以及是否有新的日志需要发送, 没有就直接发送心跳
			if req.PrevLogIndex < rf.lastIncludeIndex {
				log.Printf("leader %d send server %d 【snapshot】 instead of heartbeat\n", rf.me, i)
				sendInstallSnapShot = true
			} else if rf.VirtualLogIndex(len(rf.log)-1) > req.PrevLogIndex {
				log.Printf("leader %d send server %d 【newlogs】 instead of heartbeat\n", rf.me, i)
				req.Entries = rf.log[rf.RealLogIndex(req.PrevLogIndex+1):]
			} else {
				log.Printf("leader %d send server %d 【heartbeat】...\n", rf.me, i)
			}

			if sendInstallSnapShot {
				go rf.HandleInstallSnapShot(i)
			} else {
				req.PreLogTerm = rf.log[rf.RealLogIndex(req.PrevLogIndex)].Term
				go rf.HandleAppendEntried(i, req)
			}
		}

		rf.mu.Unlock()
		rf.ResetHeartTimer(HeartBeatTimeOut)
	}
}

// Ticker 超时检查
// 节点启动后做后台goroutine, 检查是否超时, 超时则开始选举
// (每个节点都有的后台检查，但是选举只能除leader节点外的节点发起)
//
// Ticker() 函数放在 Make函数中
func (rf *Raft) Ticker() {
	for !rf.Killed() {
		<-rf.voteTimer.C // 超时则向通道发送数据,取消阻塞
		rf.mu.Lock()

		if rf.character != leader {
			go rf.Election() // 超时发起选举
		}
		rf.ResetVoteTimer()
		rf.mu.Unlock()
	}
}

// GetState 获取节点状态(当前任期, 自己是否是领导者)
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	return rf.term, rf.character == leader
}

// 测试程序会在每次测试后调用 Kill() 方法
// 代码中使用 Killed() 检查是否已触发终止信号
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) Killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// 将客户端命令保存到当前leader的预设日志数组
// (不保证该命令最终会被提交，因领导者可能故障或落选)
// 即使Raft实例已被终止, 本函数也应正常返回
//
// 返回值说明：
// 第一返回值 - 命令若被提交后的日志索引位置
// 第二返回值 - 当前任期号
// 第三返回值 - 当前节点是否自认为是领导者
func (rf *Raft) Start(command any) (int, int, bool) {

	rf.mu.Lock()

	defer func() {
		rf.ResetHeartTimer(15)
		rf.mu.Unlock()
	}()

	if rf.character != leader {
		return -1, -1, false
	}

	newLog := &Log{
		Term:    rf.term,
		Command: command,
	}
	rf.log = append(rf.log, *newLog)
	rf.Persist()

	return rf.VirtualLogIndex(len(rf.log) - 1), rf.term, true // 使用持续递增的索引
}

// 将Raft需要持久化的数据进行存储，以便在崩溃重启后恢复
// 记录节点需要持久化的字段, 这些字段如果有变化就需要记录
func (rf *Raft) Persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)

	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

// ReadPersist 恢复先前持久化的状态
func (rf *Raft) ReadPersist(data []byte) {
	if len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var oTerm int
	var oVotedFor int
	var oLog []Log
	var oLastIncludeIndex int
	var oLastIncludeTerm int

	// 不用加锁, 读取持久化数据时不会有其他后台协程来访问节点信息
	if d.Decode(&oTerm) != nil ||
		d.Decode(&oVotedFor) != nil ||
		d.Decode(&oLog) != nil ||
		d.Decode(&oLastIncludeIndex) != nil ||
		d.Decode(&oLastIncludeTerm) != nil {
		log.Error("Decode from persist failed...")
	} else {
		rf.term = oTerm
		rf.votedFor = oVotedFor
		rf.log = oLog

		rf.lastIncludeIndex = oLastIncludeIndex
		rf.lastIncludeTerm = oLastIncludeTerm

		rf.commitIndex = oLastIncludeIndex
		rf.lastApplied = oLastIncludeIndex

		log.Printf("Peer {%d:%d} read persist data successly!\n", rf.me, rf.term)
	}
}

// ReadSnapshot 恢复快照数据
func (rf *Raft) ReadSnapshot(data []byte) {
	if len(data) < 1 {
		return
	}
	rf.snapshot = data
	log.Printf("Peer {%d:%d} read persist snapshot successly!\n", rf.me, rf.term)
}

func (rf *Raft) LogThread() {
	for !rf.Killed() {
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()

		log.Solidf("server[me:%d], [term:%d], [commit:%d], [lastIncludeIndex:%d], [logs:%+v]\n", rf.me, rf.term, rf.commitIndex, rf.lastIncludeIndex, rf.log)

		rf.mu.Unlock()
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.log = make([]Log, 0)
	rf.log = append(rf.log, Log{Term: 0})

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.character = follower
	rf.applyCh = applyCh
	rf.condApply = sync.NewCond(&rf.mu)
	rf.rands = rand.New(rand.NewSource(int64(rf.me)))
	rf.voteTimer = time.NewTimer(0)
	rf.heartTimer = time.NewTimer(0)

	rf.ResetVoteTimer()

	// 从持久化存储中读取崩溃前的状态进行恢复
	rf.ReadPersist(persister.ReadRaftState())
	rf.ReadSnapshot(persister.ReadSnapshot())

	// Index 从1开始计算
	log.Printf("Peer %d Start...", me)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.VirtualLogIndex(len(rf.log))
	}

	// 持有rf实例, goroutine内部只有节点被killed才能return, 否则一直存在
	go rf.Ticker()
	go rf.CommitChecker()
	go rf.LogThread()

	return rf
}
