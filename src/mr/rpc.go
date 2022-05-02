package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type ApplyTaskArgs struct {
	workerId string
}

type ApplyTaskReply struct {
	taskType  string
	taskIndex  int
	inputFiles []string
	nMap       int  // 获取map中间文件使用
	nReduce   int  // hash使用
	end       bool // 是否已结束
}

type NotifyFinishArgs struct {
	taskType  string
	taskIndex int
	workId    string
	tmpFiles  []string // 生成的临时文件名称
}

type NotifyFinishReply struct {
	success bool
}

// Cook up a unique-ish UNIX-domain socket genTaskName
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
