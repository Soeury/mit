package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

type KeyValue struct {
	Key   string
	Value string // string 便于测试
}

// 使用 ihash(key) % NReduce 来为每个由 Map 阶段输出的 KeyValue 选择对应的 Reduce 任务编号
func Ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// CallExample()
}

func CallGetTask() {
	args := GetTaskRequest{GetNull: 1}
	reply := GetTaskResponse{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("reply %+v\n", reply)
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
