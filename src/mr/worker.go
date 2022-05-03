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
	- 如果worker的此次任务判定为成功，就将中间文件的名称转正（这个操作由master做，中间文件的名字放在master保存并分发）
	- 否则清除掉这些中间文件（可以由worker自行完成，删除失败也没大影响）

*/

const tempFileDir = "./tmp"

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
		args := ApplyTaskArgs{WorkerId: workerId}
		reply := ApplyTaskReply{}
		call("Coordinator.ApplyForTask", &args, &reply)

		if reply.End {
			log.Fatalf("Worker %s received no task. Quitting...", workerId)
		}
		if reply.TaskType == Map {
			tmpFiles := handleMapTask(mapf, workerId, reply)
			notifyMaster(workerId, reply, tmpFiles)
		} else if reply.TaskType == Reduce {
			tmpFiles := handleReduceTask(reducef, workerId, reply)
			notifyMaster(workerId, reply, tmpFiles)
		}

		// 每次隔 0.5s 再请求，降低下QPS
		time.Sleep(500 * time.Millisecond)
	}

}

// 处理Map任务
func handleMapTask(mapf func(string, string) []KeyValue, workId string, reply ApplyTaskReply) []string {
	// 文件读取
	file, err := os.Open(reply.InputFiles[0])
	if err != nil {
		log.Fatalf("MapTask Worker %s cannot open %v", workId, reply.InputFiles[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("MapTask Worker %s cannot read %v", workId, reply.InputFiles[0])
	}
	file.Close()

	keyValues := mapf(reply.InputFiles[0], string(content))
	rKvMap := make(map[int][]KeyValue)

	// 确定kv应该由哪个reduce任务处理
	for _, kv := range keyValues {
		reduceIdx := ihash(kv.Key) % reply.NReduce
		rKvMap[reduceIdx] = append(rKvMap[reduceIdx], kv)
	}

	var tmpFiles = make([]string, reply.NReduce) // 生成的临时文件

	// 写入到每个中间文件
	if !existDir(tempFileDir) {
		err := os.MkdirAll(tempFileDir, 0777)
		if err != nil {
			log.Printf("Mkdir failed")
		}
	}

	for i := 0; i < reply.NReduce; i++ {
		tmpFileName := tempMOutFileName(workId)
		tmpFile, err := ioutil.TempFile(tempFileDir, tmpFileName)

		if err != nil {
			log.Fatalf("MapTask Worker %s failed to create temp file %v, err: %v", workId, tmpFileName, err)
		}
		encoder := json.NewEncoder(tmpFile)

		for _, kv := range rKvMap[i] {
			encoder.Encode(&kv)
		}

		tmpFiles[i] = tmpFile.Name()

		tmpFile.Close()
	}


	return tmpFiles
}

// 处理reduce任务
func handleReduceTask(reducef func(string, []string) string, workId string, reply ApplyTaskReply) []string {
	kvs := make([]KeyValue, 0)

	for _, mapFileName := range reply.InputFiles {
		// 文件读取
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
	tmpFileName := tempROutFile(workId)
	ofile, err := ioutil.TempFile(tempFileDir, tmpFileName)
	if err != nil {
		log.Fatalf("ReduceTask Worker %s failed to create temp file %v, err: %v", workId, tmpFileName, err)
	}

	tmpReduceFiles := []string{ofile.Name()} // 生成的临时文件

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

	return tmpReduceFiles
}

// 判断路径是否存在
func existDir(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

// 通知master：worker已完成任务。如果被master认定为执行失败，自行将临时文件删除掉
func notifyMaster(workId string, applyTaskReply ApplyTaskReply, tmpFiles []string) {
	args := NotifyFinishArgs{
		TaskType:  applyTaskReply.TaskType,
		TaskIndex: applyTaskReply.TaskIndex,
		WorkId:    workId,
		TmpFiles:  tmpFiles,
	}
	reply := NotifyFinishReply{}
	call("Coordinator.NotifyFinished", &args, &reply)

	if !reply.Success {
		// 删除中间文件
		switch applyTaskReply.TaskType {
		case Map:
			log.Printf("Map Worker %s was deemed to be failed task", workId)
			for _, tmpFile := range tmpFiles {
				if tmpFile != "" {
					os.Remove(tmpFile)
				}
			}
			break
		case Reduce:
			log.Printf("Reduce Worker %s was deemed to be failed task", workId)
			os.Remove(tmpFiles[0])
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
		log.Printf("Call %v args: %+v, reply: %+v", rpcname, args, reply)
		return true
	}

	fmt.Println(err)
	return false
}
