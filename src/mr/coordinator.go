package mr

import (
	"6.824/util"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

/*
主要功能：
- 分配任务
- 发现过期worker并重新分配对应任务

*/

type Coordinator struct {
	// Your definitions here.
	mu        sync.Mutex      // 读写锁
	phase     string          // 当前执行阶段
	nMap      int             // map任务数
	nReduce   int             // reduce任务数
	aliveTask map[string]Task // 进行中的任务 key:taskName
	newTasks  chan Task       // 未分配的task池
}

const (
	Map    = "M"
	Reduce = "R"
)

type Task struct {
	taskType  string    // 任务类型
	index     int       // 任务生成时的排序
	taskName  string    // 任务名，type-index，全局唯一
	inputFile string    // 需要读取的文件
	workerId  string    // 任务对应的 worker id
	ddl       time.Time // 预期完成时间
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mu:        sync.Mutex{},
		phase:     Map,
		nMap:      len(files),
		nReduce:   nReduce,
		aliveTask: make(map[string]Task),
		newTasks:  make(chan Task, util.Max(len(files), nReduce)),
	}

	// 生成Map任务
	for i, f := range files {
		t := Task{
			taskType:  Map,
			inputFile: f,
			index:     i,
			taskName:  fmt.Sprintf("%v-%v", Map, i),
		}
		c.newTasks <- t
	}

	// 监听
	c.server()

	// 定期检查超时任务
	go func() {
		time.Sleep(1 * time.Second)
		c.checkDdl()
	}()

	return &c
}

// 检查超时的任务，使其失效并重新加入任务池
func (c *Coordinator) checkDdl() {
	for _, t := range c.aliveTask {
		if t.taskName != "" && t.ddl.After(time.Now()) {
			c.mu.Lock()

			log.Printf("Found time-out task, type: %s, taskName: %s, workerId: %s, ", t.taskType, t.taskName, t.workerId)
			delete(c.aliveTask, t.taskName)
			t.workerId = ""
			t.ddl = time.Time{}
			c.newTasks <- t

			c.mu.Unlock()
		}
	}
}

// todo
//ApplyForTask 如果没有任务，返回一个空的标志，告知worker可以停止
func (c *Coordinator) ApplyForTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	t, ok := <-c.newTasks
	if !ok {
		reply.end = true
		return nil
	}

	c.mu.Lock()

	t.workerId = args.workerId
	t.ddl = time.Now().Add(10 * time.Second)
	c.aliveTask[t.taskName] = t

	c.mu.Unlock()

	reply.nMap = c.nMap
	reply.nReduce = c.nReduce
	reply.end = false

	return nil
}

//NotifyFinished worker通知master任务已完成
func (c *Coordinator) NotifyFinished(args *NotifyFinishArgs, reply *NotifyFinishReply) error {
	//if args == nil {
	//	return errors.New("nil args")
	//}
	//if reply == nil {
	//	return errors.New("nil reply")
	//}
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果未超时，返回成功。否则视为失败，重新分配该任务

	return nil
}

// 输出文件格式 mr-out-X

// Example
// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	if c.phase == "" {
		return true
	}

	return false
}
