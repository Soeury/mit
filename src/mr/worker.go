package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	log "mit/log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ByKey 类型用于 sort
type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

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

	// wd, _ := os.Getwd()
	// log.Debugf("current work place: %s", wd)

	// 1. 打开指定文件 -> 读取文件
	mapFile, err := os.Open(fileName)
	if err != nil {
		log.Errorf("open file %s failed\n", fileName)
	}

	content, err := io.ReadAll(mapFile)
	if err != nil {
		log.Errorf("read file %s failed\n", fileName)
	}
	defer mapFile.Close()

	// 2. map 处理文件 -> 得到中间数据
	kva := mapf(fileName, string(content))
	intermediate := []KeyValue{}
	intermediate = append(intermediate, kva...)

	// 3. 创建指定名称文件 -> 中间数据写入(怎么写? json.Encode)
	filenames := make([]string, reduceNum)
	files := make([]*os.File, reduceNum)

	var i int64 = 0
	for ; i < reduceNum; i++ {
		mr := "mr"
		middleName := mr + "-" + taskID + "-" + strconv.Itoa(int(i))
		middleFile, err := os.Create(middleName)
		if err != nil {
			log.Errorf("create file %s failed\n", middleName)
		}
		files[i] = middleFile
		filenames[i] = middleName
	}

	for _, kv := range intermediate {
		index := int64(Ihash(kv.Key)) % reduceNum
		json.NewEncoder(files[index]).Encode(&kv)
	}

	// 4. 返回中间文件名称
	return filenames
}

func HandleReduce(reducef func(string, []string) string, middleFileNames []string) {

	// 1. 打开中间文件 -> 从中间文件中读取源数据
	files := make([]*os.File, len(middleFileNames))
	intermediate := []KeyValue{}
	var err error

	for index, name := range middleFileNames {
		files[index], err = os.Open(name)
		if err != nil {
			log.Errorf("open file %s failed\n", name)
		}

		defer files[index].Close()
		decoder := json.NewDecoder(files[index])
		for {
			var kv KeyValue
			if err = decoder.Decode(&kv); err != nil {
				if err != io.EOF {
					log.Errorf("decode file %s failed\n", name)
				}
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// 2. 排序  -> 剪切字符串获取索引 -> 创建最终结果文件
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-"
	index := middleFileNames[0][strings.LastIndex(middleFileNames[0], "-")+1:]
	oname = oname + index
	ofile, err := os.Create(oname)
	if err != nil {
		log.Errorf("create file %s failed\n", oname)
	}

	// 3. reduce 处理数据(参考 mrsequential) -> 处理数据写入最终结果文件
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		resp := GetTaskResponse{}
		time.Sleep(time.Second)
		ok := CallGetTask(&resp)
		if !ok {
			return
		}

		reportReq := ReportRequest{
			TaskID:          resp.TaskID,
			MiddleFileNames: make([]string, 0),
		}

		// 条件无效原因:
		// CallGetTask 和 CallReport 传入参数时必须传入指针!
		switch resp.TaskType {
		case TypeMap:
			middleFiles := HandleMap(mapf, resp.MFileName, resp.ReduceNum, resp.TaskID)
			reportReq.MiddleFileNames = middleFiles
			CallReport(&reportReq)
		case TypeReduce:
			HandleReduce(reducef, resp.RFileNames)
			CallReport(&reportReq)
		case TypeSleep:
			time.Sleep(DefaultTimeDuration)
		case TypeExit:
			return
		default:
			log.Error("get task is invalid type")
		}
	}
}

func CallGetTask(resp *GetTaskResponse) bool {
	req := &GetTaskRequest{GetNull: DefaultGetNull}
	return call("Coordinator.GetTask", req, resp)
}

func CallReport(req *ReportRequest) {
	resp := &ReportResponse{ReportNull: DefaultReportNull}
	call("Coordinator.Report", req, resp)
}

// call: 向 Coordinator 发送请求
func call(rpcname string, args any, reply any) bool {
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:8090")
	if err != nil {
		log.Error("dialing failed:", err)
		return false
	}
	defer client.Close()

	err = client.Call(rpcname, args, reply)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return true
}
