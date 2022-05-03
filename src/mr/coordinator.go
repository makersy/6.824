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
	mu          sync.Mutex       // 读写锁
	phase       string           // 当前执行阶段
	nMap        int              // map任务数
	nReduce     int              // reduce任务数
	aliveTask   map[string]Task  // 进行中的任务 key:genTaskName
	newTasks    chan Task        // 未分配的task池
	mapOutFiles map[int][]string // map任务生成的中间文件。key: reduce task index，value: intermediate files
}

const (
	Map    = "M"
	Reduce = "R"
)

type Task struct {
	taskType   string    // 任务类型
	index      int       // 任务生成时的排序
	taskName   string    // 任务名，type-index，全局唯一
	inputFiles []string  // 需要读取的文件
	workerId   string    // 任务对应的 worker id
	ddl        time.Time // 预期完成时间
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mu:          sync.Mutex{},
		phase:       Map,
		nMap:        len(files),
		nReduce:     nReduce,
		aliveTask:   make(map[string]Task),
		newTasks:    make(chan Task, util.Max(len(files), nReduce)),
		mapOutFiles: make(map[int][]string, nReduce),
	}

	// 生成Map任务
	for i, f := range files {
		t := Task{
			taskType:   Map,
			inputFiles: []string{f},
			index:      i,
			taskName:   genTaskName(Map, i),
		}
		c.newTasks <- t
	}

	log.Printf("MakeCoordinator success. new tasks number: %d", len(c.newTasks))

	// 监听
	c.server()

	// 定期检查超时任务
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.mu.Lock()
			if c.phase == "" {
				return
			}
			c.mu.Unlock()
			c.checkDdl()
		}
	}()

	return &c
}

func genTaskName(taskType string, index int) string {
	return fmt.Sprintf("%v-%v", taskType, index)
}

// 检查超时的任务，使其失效并重新加入任务池
func (c *Coordinator) checkDdl() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range c.aliveTask {
		if t.taskName != "" && time.Now().After(t.ddl) {
			log.Printf("Found time-out task, type: %s, genTaskName: %s, WorkerId: %s", t.taskType, t.taskName, t.workerId)
			c.deleteAndRebuildTask(&t)
		}
	}
}

func (c *Coordinator) deleteAndRebuildTask(t *Task) {
	delete(c.aliveTask, t.taskName)
	t.workerId = ""
	t.ddl = time.Time{}
	c.newTasks <- *t
}

//ApplyForTask 如果所有任务都已被成功执行，则返回end=true，告知worker可以停止
func (c *Coordinator) ApplyForTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	t, ok := <-c.newTasks
	if !ok {
		reply.End = true
		return nil
	}

	c.mu.Lock()

	t.workerId = args.WorkerId
	t.ddl = time.Now().Add(10 * time.Second)
	c.aliveTask[t.taskName] = t

	reply.TaskIndex = t.index
	reply.TaskType = t.taskType
	reply.InputFiles = t.inputFiles
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	reply.End = false

	c.mu.Unlock()

	return nil
}

//NotifyFinished worker通知master任务已完成。如果当前阶段所有任务均已完成，切换master执行阶段
func (c *Coordinator) NotifyFinished(args *NotifyFinishArgs, reply *NotifyFinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(args.TmpFiles) == 0 {
		log.Printf("NotifyFinished failed because got empty args.TmpFiles")
		reply.Success = false
		return nil
	}

	// 如果未超时，返回成功。否则视为失败，重新分配该任务
	task, ok := c.aliveTask[genTaskName(args.TaskType, args.TaskIndex)]

	// 任务池没有，说明已经被后台任务认定超时，删除掉了
	if !ok {
		log.Printf("NotifyFinished failed because got empty args.TmpFiles")
		reply.Success = false
		return nil
	}

	// 超时
	if time.Now().After(task.ddl) {
		c.deleteAndRebuildTask(&task)
	}

	// 将中间文件转正
	switch task.taskType {
	case Map:
		for i, tmpFile := range args.TmpFiles {
			if tmpFile == "" {
				log.Printf("Found nil temp map file. WorkerId: %s, map index: %v, reduce index: %v",
					args.WorkId, args.TaskIndex, i)
				continue
			}
			name := mOutFileName(args.TaskIndex, i)
			err := os.Rename(tmpFile, name)
			if err != nil {
				log.Printf("Failed to rename map file from %s to %s", tmpFile, name)
				c.deleteAndRebuildTask(&task)
				reply.Success = false
				return nil
			}
			c.mapOutFiles[i] = append(c.mapOutFiles[i], name)
		}
		break
	case Reduce:
		tempName := args.TmpFiles[0]
		name := rOutFileName(args.TaskIndex)
		err := os.Rename(tempName, name)
		if err != nil {
			log.Printf("Failed to rename reduce file from %s to %s", tempName, name)
			c.deleteAndRebuildTask(&task)
			reply.Success = false
			return nil
		}
		log.Printf("Succeed to rename reduce file from %s to %s", tempName, name)
		break
	default:
		break
	}

	delete(c.aliveTask, task.taskName)
	reply.Success = true

	if len(c.aliveTask) == 0 && len(c.newTasks) == 0 {
		// 新开一个协程去切状态，不增加此次rpc时间
		go c.changePhase()
	}

	return nil
}

// 切换master执行阶段。当前是map阶段，则生成reduce任务；当前是reduce阶段，则关闭newtask channel
func (c *Coordinator) changePhase() {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("Changing phase from %v", c.phase)

	switch c.phase {
	case Map:
		for i := 0; i < c.nReduce; i++ {
			t := Task{
				taskType:   Reduce,
				index:      i,
				taskName:   genTaskName(Reduce, i),
				inputFiles: c.mapOutFiles[i],
				ddl:        time.Now().Add(10 * time.Second),
			}
			c.newTasks <- t
		}
		c.phase = Reduce
		break
	case Reduce:
		c.phase = ""
		close(c.newTasks)
		break
	default:
		break
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == "" {
		log.Printf("Done")
		return true
	}

	return false
}
