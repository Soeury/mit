package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

// 使用 ihash(key) % NReduce 来为每个由 Map 阶段输出的 KeyValue 选择对应的 Reduce 任务编号
func Ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func HandleMap(mapf func(string, string) []KeyValue, fileName string, reduceNum int64, taskID string) []string {
	return nil
}

func HandleReduce(reducef func(string, []string) string, filenames []string) string {
	return ""
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		resp := GetTaskResponse{}
		CallGetTask(resp)

		if resp.TaskType == TypeMap {
			middleFiles := HandleMap(mapf, resp.MFileName, resp.ReduceNum, resp.TaskID)
			reportReq := ReportRequest{}
			reportReq.MiddleFileNames = middleFiles
			reportReq.TaskID = resp.TaskID
			CallReport(reportReq)
		} else if resp.TaskType == TypeReduce {
			// 任务类型为 reduce 类型
		} else if resp.TaskType == TypeSleep {
			time.Sleep(DefaultTimeDuration)
		} else {
			log.Fatal("get task is invalid type")
		}
	}
}

func CallGetTask(resp GetTaskResponse) {
	req := GetTaskRequest{GetNull: DefaultGetNull}
	ok := call("Coordinator.GetTask", &req, &resp)
	if ok {
		fmt.Printf("reply %+v\n", resp)
	} else {
		fmt.Printf("call failed\n")
	}
}

func CallReport(req ReportRequest) {
	resp := ReportResponse{ReportNull: DefaultReportNull}
	ok := call("Coordinator.Report", &req, &resp)
	if ok {
		fmt.Printf("reply %+v\n", resp)
	} else {
		fmt.Printf("call failed\n")
	}
}

// call: 向 Coordinator 发送请求
func call(rpcname string, args any, reply any) bool {
	sockname := coordinatorSock()
	client, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing failed:", err)
	}
	defer client.Close()

	err = client.Call(rpcname, args, reply)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return true
}
