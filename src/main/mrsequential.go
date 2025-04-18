package main

import (
	"fmt"
	"io"
	"log"
	"mit/mr"
	"os"
	"plugin"
	"sort"
)

// 被排序的类型需要实现 Sort.Interface 接口的三个方法:
// Len   Swap   Less
// Less 规定排序规则: 按照 Key 的字典序进行排序

type ByKey []mr.KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

// 一个非并发的 MapReduce 例子，单机将所有结果连续执行统计到一个文件 "mr-out-0" 中
func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	// go run -race mrsequential.go wc.so pg*.txt
	// Args[1]:  插件文件名
	// Args[2:]: 文本文件名

	mapf, reducef := loadPlugin(os.Args[1])
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	// 文本中所有的字母字符以 kv 结构体形式存储在 intermediate 中

	sort.Sort(ByKey(intermediate))
	oname := "mr-out-0"
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("create file %s failed", oname)
	}

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// reducef 统计相同 Key 的个数，values 存储所有 Key 相同的结构体
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	ofile.Close()
}

func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
