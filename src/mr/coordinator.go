package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TypeMap = iota
	TypeReduce
	TypeSleep
)

const (
	TaskUnexecuted = iota
	TaskExecuting
	TaskFinished
)

const (
	StatusHealthy = iota
	StatusTimeout
)

// 全局唯一的 ID 作为任务的名称
var (
	UniqueTaskID      int64 = 0
	DefaultReduceID   int64 = -1
	DefaultReportNull int64 = 1
)

type Task struct {
	ID        string // 任务名称
	Type      int64  // 任务类型
	Status    int64  // 任务状态: 0-正常  1-超时
	MFileName string // 记录分配给 Map 任务的文件名称
	RFileID   int64  // 记录分配给 Reduce 任务的文件编号
}

// MRecords:     map  [filename]  status
// RRecords:     map  [ReduceID]  status
// TaskMap:      map  [TaskID]    *Task
// MiddleFiles:  map  [ReduceID]  []string
type Coordinator struct {
	Mutex       sync.Mutex
	MRecords    map[string]int64   // map:    0-未执行 1-执行中 2-已完成
	RRecords    map[int64]int64    // reduce: 0-未执行 1-执行中 2-已完成
	MiddleFiles map[int64][]string // 记录中间文件
	TaskMap     map[string]*Task   // 记录正在执行的任务
	ReduceNum   int64              // 记录 reduce 任务的数量
	MCount      int64              // 已完成的 map 任务数量
	RCount      int64              // 已完成的 reduce 任务数量
	MIsFinished bool               // 标记所有 map 任务是否已完成
	RIsFinished bool               // 标记所有 reduce 任务是否已完成
}

func (c *Coordinator) Report(req *ReportRequest, resp *ReportResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	resp.ReportNull = DefaultReportNull

	// 在任务池中, 检查状态, 判断任务类型
	if task, ok := c.TaskMap[req.TaskID]; ok {
		status := task.Status
		// 超时, 从任务池中删除
		if status == StatusTimeout {
			delete(c.TaskMap, req.TaskID)
			return nil
		}

		typ := task.Type
		if typ == TypeMap {
			MFilename := task.MFileName
			c.MRecords[MFilename] = TaskFinished
			c.MCount += 1
			if c.MCount == int64(len(c.MRecords)) {
				c.MIsFinished = true
			}

			// 处理上报的中间数据
			for _, value := range req.MiddleFileNames {
				index := strings.LastIndex(value, "-")
				num, err := strconv.Atoi(value[index+1:])
				if err != nil {
					log.Fatal(err)
				}
				c.MiddleFiles[int64(num)] = append(c.MiddleFiles[int64(num)], value)
			}

			delete(c.TaskMap, task.ID)
			return nil
		} else if typ == TypeReduce {
			reduceID := task.RFileID
			c.RRecords[reduceID] = TaskFinished
			c.RCount += 1
			delete(c.TaskMap, task.ID)

			if c.RCount == c.ReduceNum {
				c.RIsFinished = true
			}
			return nil
		} else {
			log.Fatal("task type is not map or reduce")
		}
	}

	log.Printf("%s task isn't in task map\n", req.TaskID)
	return nil
}

func (c *Coordinator) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	// 多 worker 竞争
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// 响应数据初始化
	resp.MFileName = ""
	resp.RFileData = make([]string, 0)
	resp.ReduceNum = c.ReduceNum
	resp.TaskID = strconv.Itoa(int(UniqueTaskID))
	UniqueTaskID = UniqueTaskID + 1

	if c.MIsFinished {
		// key: ID
		for RID, status := range c.RRecords {
			if status == TaskExecuting || status == TaskFinished {
				continue
			}
			c.RRecords[RID] = TaskExecuting
			resp.RFileData = append(resp.RFileData, c.MiddleFiles[RID]...)
			resp.TaskType = TypeReduce

			task := &Task{
				ID:        resp.TaskID,
				Type:      resp.TaskType,
				Status:    StatusHealthy,
				MFileName: "",
				RFileID:   RID,
			}

			c.TaskMap[resp.TaskID] = task
			go c.HandleTimeout(resp.TaskID)
			return nil
		}
		resp.TaskType = TypeSleep
	} else {
		// key: Filename
		for fileName, status := range c.MRecords {
			if status == TaskExecuting || status == TaskFinished {
				continue
			}
			c.MRecords[fileName] = TaskExecuting
			resp.MFileName = fileName
			resp.TaskType = TypeMap

			task := &Task{
				ID:        resp.TaskID,
				Type:      resp.TaskType,
				Status:    StatusHealthy,
				MFileName: resp.MFileName,
				RFileID:   DefaultReduceID,
			}

			c.TaskMap[resp.TaskID] = task
			go c.HandleTimeout(resp.TaskID)
			return nil
		}

		// 未分配任务
		resp.TaskType = TypeSleep
	}
	return nil
}

func (c *Coordinator) HandleTimeout(TaskID string) {
	// 给 10s 处理时间
	time.Sleep(time.Second * 10)
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// worker 处理完任务后需要 report, 从任务池中移除任务
	// 10s 后还在执行说明任务超时, 重置任务状态
	if task, ok := c.TaskMap[TaskID]; ok {
		task.Status = StatusTimeout

		if task.Type == TypeMap {
			Filename := task.MFileName
			if c.MRecords[Filename] == TaskExecuting {
				c.MRecords[Filename] = TaskUnexecuted
			}
		} else if task.Type == TypeReduce {
			FileID := task.RFileID
			if c.RRecords[FileID] == TaskExecuting {
				c.RRecords[FileID] = TaskUnexecuted
			}
		}
	}
}

// 注册 Coordinator, 将 RPC 消息处理器绑定到 HTTP 路由
// 使用 Unix 域套接字: 仅允许本地进程通信 (无需暴露网络端口)
// 持续接受连接并处理 RPC or HTTP 请求
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	listener, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("listen error:", err)
	}

	go http.Serve(listener, nil)
}

// Done 用于检查整个任务是否已经完成
func (c *Coordinator) Done() bool {
	ret := false
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.RCount == c.ReduceNum {
		ret = true
	}
	return ret
}

// Coordinator 初始化
func MakeCoordinator(files []string, nReduce int64) *Coordinator {
	c := Coordinator{
		Mutex:       sync.Mutex{},
		MRecords:    make(map[string]int64),
		RRecords:    make(map[int64]int64),
		MiddleFiles: make(map[int64][]string),
		TaskMap:     make(map[string]*Task),
		ReduceNum:   nReduce,
		MCount:      0,
		RCount:      0,
		MIsFinished: false,
		RIsFinished: false,
	}

	for _, file := range files {
		c.MRecords[file] = 0
	}
	var ReduceID int64 = 0
	for ; ReduceID < nReduce; ReduceID++ {
		c.RRecords[ReduceID] = 0
	}

	// MiddleFiles 的数据在 map 任务执行后产生, 目前为空
	c.server()
	return &c
}
