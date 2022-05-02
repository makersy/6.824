package mr

import (
	json "encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

/*
worker 的工作：
1. 向 coordinator 发送rpc，申请任务
2. 拿到返回值后：
	- 如果返回空，那么说明所有任务已被执行完，结束worker
	- 如果返回了一个 Map 任务，那么：
		- 读取输入文件的内容
		- 调用 Map 函数拿到中间数据
		- 对中间数据的每个 key 调用 hash，根据 hash 值得出该 kv 对应该由哪个 reduce 任务处理，保存到该 map 任务对应的 reduce 中间文件
	- 如果返回了一个 Reduce 任务，那么：
		- 读取每个map任务生成的关于这个reduce任务的中间文件
		- 按 key 进行归并、排序
		- 输出最终结果到 reduce 文件
	- 总结下，假设有15个map任务、10个reduce任务，那么每个map生成的中间文件应该有10个，每个reduce处理15个中间文件
3. 报告执行情况，得到master返回值。
	- 如果worker的此次任务判定为成功，就将中间文件的名称转正（这个操作应该由master做，否则可能会出现master已经分配给了reduce任务，但是map worker还没能
		rename成功的情形，此时reduce任务就执行失败了）；
	- 否则清除掉这些中间文件（可以由worker自行完成，删除失败也没影响）

*/

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerId := strconv.Itoa(os.Getpid())

	for {
		// 获取任务
		args := ApplyTaskArgs{workerId: workerId}
		reply := ApplyTaskReply{}
		call("Coordinator.ApplyForTask", &args, &reply)

		if reply.end {
			log.Printf("Worker %s received no task. Quitting...", workerId)
			break
		}
		if reply.taskType == Map {
			handleMapTask(mapf, workerId, reply)
			notifyMaster(workerId, reply)
		} else if reply.taskType == Reduce {
			handleReduceTask(reducef, workerId, reply)
			notifyMaster(workerId, reply)
		}

		// 每次隔 0.5s 再请求，降低下QPS
		time.Sleep(500 * time.Millisecond)
	}

}

// 处理Map任务
func handleMapTask(mapf func(string, string) []KeyValue, workId string, reply ApplyTaskReply) {
	// 文件读取
	file, err := os.Open(reply.inputFile)
	if err != nil {
		log.Fatalf("MapTask Worker %s cannot open %v", workId, reply.inputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("MapTask Worker %s cannot read %v", workId, reply.inputFile)
	}
	file.Close()

	keyValues := mapf(reply.inputFile, string(content))
	rKvMap := make(map[int][]KeyValue)

	// 确定kv应该由哪个reduce任务处理
	for _, kv := range keyValues {
		reduceIdx := ihash(kv.Key) % reply.nReduce
		rKvMap[reduceIdx] = append(rKvMap[reduceIdx], kv)
	}

	// 写入到每个中间文件
	for i := 0; i < reply.nReduce; i++ {
		tmpFileName := tempMapOutFile(workId, reply.taskIndex, i)
		tmpFile, err := os.Create(tmpFileName)

		if err != nil {
			log.Fatalf("MapTask Worker %s failed to create intermediate file %v", workId, tmpFileName)
		}
		encoder := json.NewEncoder(tmpFile)

		for _, kv := range rKvMap[i] {
			encoder.Encode(&kv)
		}

		tmpFile.Close()
	}
}

// 处理reduce任务
func handleReduceTask(reducef func(string, []string) string, workId string, reply ApplyTaskReply) {
	kvs := make([]KeyValue, 0)

	for mIdx := 0; mIdx < reply.nMap; mIdx++ {
		// 文件读取
		mapFileName := mapOutFile(mIdx, reply.taskIndex)
		mapFile, err := os.Open(mapFileName)
		if err != nil {
			log.Fatalf("ReduceTask Worker %s cannot open %v", workId, mapFileName)
		}

		// 解析kv
		decoder := json.NewDecoder(mapFile)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}

		// 关闭文件
		mapFile.Close()
	}

	// 排序
	sort.Sort(ByKey(kvs))

	// 归并 & 输出
	oname := tempReduceOutFile(workId, reply.taskIndex)
	ofile, _ := os.Create(oname)

	for i := 0; i < len(kvs); {
		// 归并
		key := kvs[i].Key
		values := make([]string, 1)
		for i < len(kvs) {
			if kvs[i].Key == key {
				values = append(values, kvs[i].Value)
				i++
			} else {
				break
			}
		}
		reduceRes := reducef(key, values)

		// 输出
		fmt.Fprintf(ofile, "%v %v\n", key, reduceRes)
	}

	ofile.Close()
}

// 通知master：worker已完成任务
func notifyMaster(workId string, applyTaskReply ApplyTaskReply) {
	args := NotifyFinishArgs{
		taskType:  applyTaskReply.taskType,
		taskIndex: applyTaskReply.taskIndex,
		workId:    workId,
	}
	reply := NotifyFinishReply{}
	call("Coordinator.NotifyFinished", &args, &reply)

	if !reply.success {
		// 删除中间文件
		switch applyTaskReply.taskType {
		case Map:
			log.Printf("Map Worker %s was deemed to be failed task", workId)
			for i := 0; i < applyTaskReply.nReduce; i++ {
				os.Remove(tempMapOutFile(workId, applyTaskReply.taskIndex, i))
			}
			break
		case Reduce:
			log.Printf("Reduce Worker %s was deemed to be failed task", workId)
			os.Remove(tempReduceOutFile(workId, applyTaskReply.taskIndex))
			break
		default:
			break
		}
	}
}

// CallExample
// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
