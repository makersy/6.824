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
	aliveTask map[string]Task // 进行中的任务
	newTasks  chan Task       // 未分配的task池
}

const (
	Map    = "M"
	Reduce = "R"
)

type Task struct {
	taskType string    // 任务类型
	taskName string    // 任务名，基于 type 和 index 唯一生成
	file     string    // 文件名
	workerId int       // 执行的worker id
	ddl      time.Time // 预期完成时间
}

// Your code here -- RPC handlers for the worker to call.

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
			taskType: Map,
			file:     f,
			taskName: fmt.Sprintf("%v-%v", Map, i),
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

			log.Printf("Found time-out task, type: %s, taskName: %s, workerId: %d, ", t.taskType, t.taskName, t.workerId)
			delete(c.aliveTask, t.taskName)
			t.workerId = -1
			t.taskName = ""
			c.newTasks <- t

			c.mu.Unlock()
		}
	}
}

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
	ret := false

	// Your code here.

	return ret
}
