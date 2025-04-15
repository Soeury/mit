package mr

import (
	"os"
	"strconv"
)

// 定义的是发送的请求与返回的响应参数, 供客户端使用
type GetTaskRequest struct {
	GetNull int64
}

// 返回需要被 worker 处理的数据
type GetTaskResponse struct {
	MFileName string   // Map 文件名
	RFileData []string // Reduce 文件名
	TaskID    string   // 任务名称(唯一编号)
	TaskType  int64    // 任务类型: 0-map 1-reduce 2-sleep
	ReduceNum int64    // Reduce 任务数量
}

type ReportRequest struct {
	TaskID          string   // 任务名称(唯一编号)
	MiddleFileNames []string // 完成 map 任务后上报需提供中间文件名称
}

type ReportResponse struct {
	ReportNull int64
}

// 在 /var/tmp 目录下生成一个（相对）独特的 UNIX 域套接字名称供 coordinator server 使用
func coordinatorSock() string {
	return "/var/tmp/824-mr-" + strconv.Itoa(os.Getuid())
}
