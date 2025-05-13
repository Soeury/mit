package raft

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"mit/labgob"
	"mit/labrpc"
	log "mit/log"
	mlog "mit/log"
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
	CommandValid bool // 区分日志(true)和快照(false)
	Command      any  // 日志内容
	CommandIndex int  // 日志索引

	SnapshotValid bool   // 是否启用快照
	Snapshot      []byte // 快照数据
	SnapshotTerm  int    //
	SnapshotIndex int
}

type Log struct {
	Term       int  // 日志所属任期
	Index      int  // 日志在数组中的位置索引
	Command    any  // 日志内容
	IsHearBeat bool // 是否为心跳(用于心跳)
	IsEmpty    bool // 是否为空(用于系统同步)
}

type Raft struct {
	mu        sync.Mutex          // 并发锁
	peers     []*labrpc.ClientEnd // 所有节点的 RPC 终端
	persister *Persister          // 持久化存储 [persister.go]
	me        int                 // 本节点在 peers 中的索引
	dead      int32               // 由 Kill 设置的终止标志
	character int                 // 节点角色
	loseBount int                 // 配合心跳使用, 一个单位未收到心跳就自增
	logCount  int                 // 日志计数? 是否包含空日志?
	applyCh   *chan ApplyMsg      // 日志应用到状态机的通道

	term     int   // 当前任期
	votedFor int   // 当前任期投票给的 candidate ID
	log      []Log // 预设日志数组

	commitIndex int // 可以提交的最高日志索引
	lastApplied int // 已应用的最高日志索引

	lastIncludeIndex int // 快照包含的最高日志索引
	lastIncludeTerm  int // 快照最高日志的任期

	nextIndex  []int // leader 下的每个 follower 的下一个待同步的日志索引
	matchIndex []int // leader 下的每个 follower 和 Leader 一致的日志最大索引
}

const (
	leader = iota
	candidate
	follower
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
	Term        int  // 接收者当前任期
	Success     bool // 接收者是否同步成功
	CommitIndex int  // 接收者已提交的最高日志索引
}

type InstallSnapshotRequest struct {
	Term             int    // leader 当前任期
	LeaderID         int    // leader ID: 节点的 me 值
	LastIncludeIndex int    // 快照包含的最后一个日志的索引
	LastIncludeTerm  int    // 最后一个日志的任期
	Data             []byte // 快照原始字节数据
}

type InstallSnapshotResponse struct {
	Term        int  // 接收者的任期
	Success     bool // 是否接收了快照
	CommitIndex int  // 接收者最后提交的日志索引
}

func Max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

func Min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

// leader向follower同步日志
func (rf *Raft) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Term >= rf.term {
		// leader的term大于当前节点
		rf.term = req.Term
		if rf.character != follower {
			rf.BecomeFollowerWithLock(rf.term)
			rf.votedFor = req.LeaderID
		}
		rf.Persist()
	} else {
		// leader的term小于当前节点
		resp.Term = rf.term
		resp.Success = false
		resp.CommitIndex = rf.commitIndex
		return
	}
	resp.Term = rf.term
	resp.CommitIndex = rf.commitIndex

	if len(req.Entries) == 0 {
		// 收到心跳日志 -> 重置定时器
		if rf.commitIndex < req.CommitIndex {
			if req.PrevLogIndex < len(rf.log) && rf.log[req.PrevLogIndex].Term == req.PreLogTerm {
				rf.commitIndex = Min(req.PrevLogIndex, req.CommitIndex)
			}
		}
		rf.loseBount = 0
		resp.Success = true
		rf.Persist()
		return
	} else {
		if rf.commitIndex >= req.PrevLogIndex+len(req.Entries) {
			// 在已提交范围内
			resp.Success = true
			return
		}
		if req.PrevLogIndex >= len(rf.log) || rf.log[req.PrevLogIndex].Term != req.PreLogTerm {
			// 日志与leader前一条无法匹配
			resp.Success = false
		} else {
			rf.log = rf.log[0 : req.PrevLogIndex+1]
			rf.log = append(rf.log, req.Entries...)
			resp.Success = true
			if rf.commitIndex < req.CommitIndex { // 重点
				// 最终提交进度不超过 req.CommitIndex
				// req.CommitIndex > or < req.PreLogIndex ? 数轴考虑
				rf.commitIndex = Min(req.CommitIndex, Max(req.PrevLogIndex, rf.commitIndex))
			}
			rf.Persist()
		}
		return
	}
}

func (rf *Raft) RequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp.Term = rf.term

	// candidate term 应该比当前任期大1
	if req.Term <= rf.term {
		log.Infof("Peer {%d:%d} request vote term less than me {%d:%d}, do not vote", req.CandidateID, req.Term, rf.me, rf.term)
		resp.VoteGranted = false
		return
	} else {
		// 最后一条日志任期: candidate > follower, 可投票
		if req.LastLogTerm > rf.log[len(rf.log)-1].Term {
			log.Infof("Peer {%d:%d} request vote , log term bigger than me {%d:%d}, vote", req.CandidateID, req.Term, rf.me, rf.term)
			resp.VoteGranted = true
			rf.votedFor = req.CandidateID
			rf.BecomeFollowerWithLock(req.Term) // 改变任期，重置定时器
			rf.Persist()
			return
		} else if req.LastLogTerm == rf.log[len(rf.log)-1].Term {
			// 任期相等比较日志长度: candidate >= follower, 可投票
			if req.LastLogIndex >= len(rf.log) {
				log.Infof("Peer {%d:%d} request vote , log index bigger than me {%d:%d}, vote", req.CandidateID, req.Term, rf.me, rf.term)
				resp.VoteGranted = true
				rf.votedFor = req.CandidateID
				rf.BecomeFollowerWithLock(req.Term)
				rf.Persist()
				return
			} else {
				// 不可投票
				log.Infof("Peer {%d:%d} request vote , log is older than me {%d:%d}, do not vote", req.CandidateID, req.Term, rf.me, rf.term)
				resp.VoteGranted = false
				if rf.character == leader { // 变成 follower 改变任期重启定时器
					rf.BecomeFollowerWithLock(req.Term)
				} else {
					rf.term = req.Term
				}
				rf.Persist()
				return
			}
		} else {
			log.Infof("Peer {%d:%d} request vote , log term is older than me {%d:%d}, do not vote", req.CandidateID, req.Term, rf.me, rf.term)
			resp.VoteGranted = false
			if rf.character == leader {
				rf.BecomeFollowerWithLock(req.Term)
			} else {
				rf.term = req.Term
			}
			rf.Persist()
			return
		}
	}
}

// InstallSnapshot: follower 日志落后太多, leader 的日志已经被压缩到快照中, 无法提供给 follower
// follower 更新 leader 的快照
func (rf *Raft) InstallSnap(req *InstallSnapshotRequest, resp *InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Term < rf.term {
		resp.Term = rf.term
		resp.Success = false
		resp.CommitIndex = rf.commitIndex
		mlog.Printf("Peer {%d:%d} term is bigger , refuse this snapshot\n", rf.me, rf.term)
		return
	} else {
		rf.term = req.Term
		if rf.character != follower {
			rf.BecomeFollowerWithLock(rf.term)
			rf.votedFor = req.LeaderID
		}
		rf.Persist()

		if rf.lastIncludeIndex > req.LastIncludeIndex {
			// 已经执行过
			resp.Success = true
			resp.CommitIndex = rf.commitIndex
			return
		}

		var afterSnapLogs []Log
		if len(rf.log) >= req.LastIncludeIndex {
			afterSnapLogs = make([]Log, len(rf.log[req.LastIncludeIndex+1:len(rf.log)]))
			copy(afterSnapLogs, rf.log[req.LastIncludeIndex+1:len(rf.log)])
		}
		rf.log = afterSnapLogs
		rf.lastIncludeIndex = req.LastIncludeIndex

		// 因为缺失日志所以leader直接传递快照用于日志"同步"
		// 缺失的日志无法再被提交和应用
		// 这里将数据直接更改到最新
		// ? ? ? backwork 那边怎么办?
		if req.LastIncludeIndex > rf.commitIndex {
			rf.commitIndex = req.LastIncludeIndex
		}
		if req.LastIncludeIndex > rf.lastApplied {
			rf.lastApplied = req.LastIncludeIndex
		}
		rf.Persist()
		rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), req.Data)
		resp.Success = true
		resp.CommitIndex = rf.commitIndex
		return
	}
}

// 包装
func (rf *Raft) SendRequestVote(server int, req *RequestVoteRequest, resp *RequestVoteResponse) bool {
	return rf.peers[server].Call("Raft.RequestVote", req, resp)
}

func (rf *Raft) SendAppendEntries(server int, req *AppendEntriesRequest, resp *AppendEntriesResponse) bool {
	return rf.peers[server].Call("Raft.AppendEntries", req, resp)
}

func (rf *Raft) SendInstallSnap(server int, req *InstallSnapshotRequest, resp *InstallSnapshotResponse) bool {
	return rf.peers[server].Call("Raft.InstallSnap", req, resp)
}

// 验证快照合法性
// 当上层服务（如键值存储）希望切换至快照模式时，仅在满足以下条件时执行：
// - Raft 当前没有比快照更新的数据（即快照包含最新已提交状态）
// - 该快照已通过 applyCh 通道完成同步
// 参数: 最后的日志任期  最后的日志索引
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// Snapshot 修剪快照前的日志
// 修剪日志 更新数据
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// index 之前存在日志未提交 || index 已经执行过快照
	if rf.lastIncludeIndex > index || index > rf.commitIndex {
		return
	} else {
		term := rf.log[index].Term
		cutLog := make([]Log, len(rf.log[index+1:len(rf.log)]))
		copy(cutLog, rf.log[index+1:len(rf.log)])
		rf.log = cutLog

		rf.lastIncludeIndex = index
		rf.lastIncludeTerm = term

		if index > rf.commitIndex {
			rf.commitIndex = index
		}
		if index > rf.lastApplied {
			rf.lastApplied = index
		}
		rf.Persist()
		rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
	}
}

// SendSnapshotFunc: leader 向其他节点执行快照
func (rf *Raft) SendSnapshotFunc(peer int) {
	for {
		time.Sleep(time.Millisecond * 50)
		if rf.Killed() || rf.character != leader {
			return
		}
		// 完成快照退出?
		rf.mu.Lock()
		req := InstallSnapshotRequest{
			Term:             rf.term,
			LeaderID:         rf.me,
			LastIncludeIndex: rf.lastIncludeIndex,
			LastIncludeTerm:  rf.lastIncludeTerm,
		}
		resp := InstallSnapshotResponse{
			Term:        0,
			Success:     false,
			CommitIndex: 0,
		}
		rf.mu.Unlock()

		wg := sync.WaitGroup{}
		wg.Add(1)
		ok := false
		go func() {
			ok = rf.SendInstallSnap(peer, &req, &resp)
			wg.Done()
		}()

		var ch = make(chan bool)
		go func() {
			wg.Wait()
			ch <- true
		}()

		select {
		case <-time.After(time.Millisecond * 100):
			rf.mu.Lock()
			mlog.Printf("Peer {%d:%d} send snapshot timeout\n", rf.me, rf.term)
			rf.mu.Unlock()
		case <-ch:
			rf.mu.Lock()
			mlog.Printf("Peer {%d:%d} send snapshot finished\n", rf.me, rf.term)
			rf.mu.Unlock()
		}

		if ok {
			if resp.Success {
				rf.mu.Lock()
				mlog.Printf("Peer {%d:%d} append snapshot success\n", rf.me, rf.term)
				rf.matchIndex[peer] = req.LastIncludeIndex
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				rf.Persist()
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				if resp.Term <= rf.term {
					// 什么情况导致快照失败?
					continue
				} else {
					// 任期落后
					rf.mu.Unlock()
					return
				}
			}
		}
	}
}

// BackWork 已提交的日志应用到状态机
func (rf *Raft) BackWork() {
	for {
		time.Sleep(time.Millisecond * 50)
		if rf.Killed() {
			return
		}
		rf.mu.Lock()
		mi := make([]int, len(rf.matchIndex))
		copy(mi, rf.matchIndex)
		mi[rf.me] = len(rf.log) - 1 // 索引 -1
		sort.Ints(mi)               // 升序
		if rf.character == leader {
			midIndex := len(mi) / 2
			if rf.log[mi[midIndex]].Term == rf.term {
				// 一半节点已复制到的日志索引是当前任期的日志, 则可以提交
				rf.commitIndex = Max(mi[midIndex], rf.commitIndex)
			}
		}

		rf.Persist()

		for ; rf.lastApplied <= rf.commitIndex; rf.lastApplied++ {
			log.Infof("Peer {%d:%d} apply log %d", rf.me, rf.term, rf.lastApplied)
			if rf.log[rf.lastApplied].IsEmpty {
				continue // 系统、心跳日志跳过
			}
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.log[rf.lastApplied].Index,
			}
			*rf.applyCh <- applyMsg
		}
		rf.mu.Unlock()
	}
}

// SendAppendEntriesFunc leader当选后用于向其他节点进行同步
func (rf *Raft) SendAppendEntriesFunc(peer int) {
	for {
		time.Sleep(time.Millisecond * 50) // 间隔 50ms 同步一次
		if rf.Killed() || rf.character != leader {
			return
		}
		rf.mu.Lock()
		if rf.nextIndex[peer] >= len(rf.log) {
			rf.mu.Unlock() // 完成同步
			continue
		}
		var datas = make([]Log, len(rf.log[rf.nextIndex[peer]:len(rf.log)]))
		copy(datas, rf.log[rf.nextIndex[peer]:len(rf.log)])
		req := AppendEntriesRequest{
			Term:         rf.term,
			LeaderID:     rf.me,
			CommitIndex:  rf.commitIndex,
			PrevLogIndex: rf.nextIndex[peer] - 1,
			PreLogTerm:   rf.log[rf.nextIndex[peer]-1].Term,
			Entries:      datas,
		}
		resp := AppendEntriesResponse{
			Term:        0,
			Success:     false,
			CommitIndex: 0,
		}
		rf.mu.Unlock()

		wg := sync.WaitGroup{}
		wg.Add(1)
		ok := false
		go func() {
			ok = rf.SendAppendEntries(peer, &req, &resp)
			wg.Done()
		}()

		var ch = make(chan bool)
		go func() {
			wg.Wait()
			ch <- true
		}()

		select {
		case <-time.After(time.Millisecond * 100):
			rf.mu.Lock()
			log.Infof("Peer {%d:%d} append log to %d timeout", rf.me, req.Term, peer)
			rf.mu.Unlock()
		case <-ch:
			rf.mu.Lock()
			log.Infof("Peer {%d:%d} append log to %d finished", rf.me, req.Term, peer)
			rf.mu.Unlock()
		}

		if ok {
			if resp.Success {
				// 匹配成功, 更新 leader 记录的同步数据
				rf.mu.Lock()
				rf.matchIndex[peer] = Max(rf.matchIndex[peer], rf.nextIndex[peer]+len(datas)-1) // ?
				rf.nextIndex[peer] += len(datas)
				log.Infof("Peer {%d:%d} append log success", peer, resp.Term)
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				if resp.Term <= rf.term {
					// 任期比我小, preLog 匹配失败导致同步不成功
					log.Infof("Peer {%d:%d} append log failed, preLog match failed", peer, resp.Term)
					rf.nextIndex[peer] = resp.CommitIndex + 1 // 回退的数据量?
					rf.mu.Unlock()
				} else {
					// 当前节点任期落后
					rf.mu.Unlock()
					return
				}
			}
		}
	}
}

// 成为 follower 的所有情况如下:
// leader -> follower
//  1. leader 同步日志时发现自己的任期小于 follower 任期
//  2. leader 收到了来子新的 leader 的心跳或者日志同步请求
//  3. leader 收到了来子更大任期的 candidate 的投票请求
//
// candidate -> follower
//  4. 竞选时多数派投了反对票
//  5. 收到了任期大于等于自身任期的 leader 发送的请求
func (rf *Raft) BecomeFollowerWithLock(term int) {
	log.Infof("Peer {%d:%d} become follower", rf.me, rf.term)
	rf.loseBount = 0 // 重置定时器超时单位个数
	rf.character = follower

	// 是否初始化 ?
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.term = term

	if term > rf.term {
		rf.votedFor = -1 // 任期改变重置投票数
	}
}

// 成为 leader 的所有情况如下:
// candidate -> leader
//  1. 竞选时多数派投了赞同票
func (rf *Raft) BecomeLeaderWithLock() {
	log.Infof("Peer {%d:%d} become leader", rf.me, rf.term)
	rf.character = leader
	rf.votedFor = -1
	rf.loseBount = 0

	for i, _ := range rf.peers {
		rf.nextIndex[i] = len(rf.log) // +1 表示下一条日志索引
		rf.matchIndex[i] = 0          // 目前不知, 初始化为 0
	}

	// leader 当选后立即发送一条空日志
	logEntry := Log{
		Term:       rf.term,
		Index:      0,
		IsEmpty:    true,
		IsHearBeat: false,
		Command:    -1,
	}
	rf.log = append(rf.log, logEntry)
	rf.Persist()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		curr := i // 日志同步
		go rf.SendAppendEntriesFunc(curr)
		// go rf.SendAppendEntriesFunc(curr)
	}
}

// 成为 candidate 的所有情况如下:
// follower -> candidate
//  1. 超过一定时间未收到心跳, 自身任期+1, 成为 candidate
//
// candidate -> candidate
//  2. 在竞选时间内仍未达成有效结论, 保持身份, 自身任期+1, 开始新一轮竞选
func (rf *Raft) BecomeCandidate() {
	log.Infof("Peer {%d:%d} become candidate", rf.me, rf.term)
	for {
		t := rand.Intn(100)
		time.Sleep(time.Millisecond * time.Duration(t))
		if rf.Killed() {
			return
		}
		rf.mu.Lock()
		if rf.character == follower || rf.character == leader {
			rf.mu.Unlock()
			return
		}

		// 任期自增+给自己投票
		rf.term += 1
		rf.votedFor = rf.me
		rf.Persist()
		log.Infof("Peer {%d:%d} start vote request", rf.me, rf.term)
		rf.mu.Unlock()
		var countVote int32 = 1
		var wg = sync.WaitGroup{}

		// 向其他节点发送投票请求
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			req := &RequestVoteRequest{
				Term:         rf.term,
				CandidateID:  rf.me,
				LastLogIndex: len(rf.log),
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			resp := &RequestVoteResponse{
				Term:        0,
				VoteGranted: false,
			}
			go func() {
				ok := rf.SendRequestVote(i, req, resp)
				wg.Done()

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if resp.VoteGranted {
						atomic.AddInt32(&countVote, 1)
					} else {
						if resp.Term == rf.term {
							// 被发送节点已经投过票
						} else if resp.Term > rf.term {
							// candidate任期落后
							rf.BecomeFollowerWithLock(resp.Term)
							return
						} else {
							// 日志不完整、网络延迟投票、存在更高任期
						}
					}
				} // 发送失败?
			}()
		}

		var ch = make(chan bool)
		go func() {
			wg.Wait()
			ch <- false
		}()

		// 心跳 100ms, 选举超时大概 200-250ms
		select {
		case <-time.After(time.Millisecond * 200):
			rf.mu.Lock()
			log.Infof("Peer {%d:%d} vote rpc timeout", rf.me, rf.term)
			rf.mu.Unlock()
		case <-ch:
			rf.mu.Lock()
			log.Infof("Peer {%d:%d} vote rpc finished", rf.me, rf.term)
			rf.mu.Unlock()
		}

		rf.mu.Lock()
		if rf.character == follower {
			// 状态变更
			rf.mu.Unlock()
			return
		}
		log.Infof("Peer {%d:%d} vote count %d", rf.me, rf.term, atomic.LoadInt32(&countVote))
		if atomic.LoadInt32(&countVote)*2 > int32(len(rf.peers)) {
			// 选举成功
			rf.BecomeLeaderWithLock()
			rf.mu.Unlock()
			return
		} else {
			// 选举失败
			rf.BecomeFollowerWithLock(rf.term)
			rf.mu.Unlock()
			return
		}
	}
}

// HearBeat 心跳发送: leader 向所有节点发送心跳日志
func (rf *Raft) HearBeat() {
	// 这个 goroutine 需要一直存在, 不能退出
	// follower 和 candidate 不需要操作
	for {
		time.Sleep(time.Millisecond * 100)
		rf.mu.Lock()
		if rf.character == leader {
			l := make([]Log, 0)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				req := &AppendEntriesRequest{
					Term:         rf.term,
					LeaderID:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PreLogTerm:   rf.log[rf.nextIndex[i]-1].Term,
					CommitIndex:  rf.commitIndex,
					Entries:      l,
				}
				resp := &AppendEntriesResponse{
					Term:        0,
					Success:     false,
					CommitIndex: 0,
				}
				go func() {
					rf.SendAppendEntries(i, req, resp)
				}()
			}
		}
		rf.mu.Unlock()
		if rf.Killed() {
			return
		}
	}
}

// ticker 在当前节点长时间未收到心跳时触发新一轮选举
func (rf *Raft) ticker() {
	for {
		// 100ms 一次心跳, 收到心跳后重置心跳计数器 losebount
		time.Sleep(time.Millisecond * 100)
		if rf.Killed() {
			return
		}
		rf.mu.Lock()
		if rf.character == leader || rf.character == candidate {
			rf.mu.Unlock()
			continue
		}
		rf.loseBount += 1
		if rf.loseBount == 3 {
			// 超时3 次转换为candidate
			rf.loseBount = 0
			rf.character = candidate
			go rf.BecomeCandidate()
		}
		rf.mu.Unlock()
	}
}

// LogThread 调试日志输出循环
func (rf *Raft) LogThread() {
	for {
		time.Sleep(time.Millisecond * 100)
		if rf.Killed() {
			return
		}
		rf.mu.Lock()
		// rf.me:commitIndex[log.Term(Command), log.Term(Command)...]
		slog := strconv.Itoa(rf.me) + ":"
		slog += strconv.Itoa(rf.commitIndex)
		slog += "["
		for _, l := range rf.log {
			slog += strconv.Itoa(l.Term)
			slog += "("

			switch l.Command.(type) {
			case int:
				slog += strconv.Itoa(l.Command.(int))
			case string:
				slog += l.Command.(string)
			default:
				log.Error("log type error")
			}
			slog += "), "
		}
		slog += "]"
		log.Solid(slog)
		rf.mu.Unlock()
	}
}

// 返回当前任期(term)以及该服务器是否认为自己是领导者
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.term
	isleader = rf.character == leader
	rf.mu.Unlock()
	return term, isleader
}

// 测试程序会在每次测试后调用 Kill() 方法
// 代码中使用 Killed() 检查是否已触发终止信号
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) Killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// 将客户端命令保存到当前leader的预设数组
// (不保证该命令最终会被提交，因领导者可能故障或落选)
// 即使Raft实例已被终止, 本函数也应正常返回
//
// 返回值说明：
// 第一返回值 - 命令若被提交后的日志索引位置
// 第二返回值 - 当前任期号
// 第三返回值 - 当前节点是否自认为是领导者
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.character != leader {
		return index, term, !isLeader
	}
	// 空日志是否记录索引?
	index = len(rf.log) - rf.GetEmptyLogCountWithLock() + 1
	logEntries := Log{
		Command:    command,
		Term:       rf.term,
		IsHearBeat: false,
		IsEmpty:    false,
		Index:      index,
	}
	rf.log = append(rf.log, logEntries)
	term = rf.term
	rf.Persist()

	return index, term, isLeader
}

func (rf *Raft) GetEmptyLogCountWithLock() int {
	EmptyLogNum := 0
	for _, log := range rf.log {
		if log.IsEmpty {
			EmptyLogNum++
		}
	}
	return EmptyLogNum
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
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// 恢复先前持久化的状态
func (rf *Raft) ReadPersist(data []byte) {
	if len(data) < 1 { // 无持久化数据
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var oTerm int
	var oVotedFor int
	var oLog []Log

	if d.Decode(&oTerm) != nil || d.Decode(&oVotedFor) != nil || d.Decode(&oLog) != nil {
		log.Error("Decode from persist failed...")
	} else {
		rf.mu.Lock()
		rf.term = oTerm
		rf.votedFor = oVotedFor
		rf.log = oLog
		log.Printf("Peer {%d:%d} read persist data: len log %d; vote %d; log", rf.me, rf.term, len(rf.log), rf.votedFor)
		rf.mu.Unlock()
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.loseBount = 0 // 心跳超时计数
	rf.character = follower
	rf.term = 1
	rf.log = make([]Log, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = &applyCh
	rf.logCount = 0

	// r := rand.New(rand.NewSource(time.Now().UnixNano()))
	log.Printf("Peer %d Start...", me)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}

	log := Log{
		Term:       0,
		IsHearBeat: false,
		IsEmpty:    true,
		Index:      0,
		Command:    -1,
	}
	rf.log = append(rf.log, log)

	// 从持久化存储中读取崩溃前的状态进行恢复
	rf.ReadPersist(persister.ReadRaftState())

	// 持有rf实例, goroutine内部只有节点被killed才能return, 否则一直存在
	go rf.ticker()
	go rf.HearBeat()
	go rf.BackWork()
	// go rf.LogThread()

	return rf
}
