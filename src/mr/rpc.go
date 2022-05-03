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
	WorkerId string
}

type ApplyTaskReply struct {
	TaskType   string
	TaskIndex  int
	InputFiles []string
	NMap       int  // 获取map中间文件使用
	NReduce    int  // hash使用
	End        bool // 是否已结束
}

type NotifyFinishArgs struct {
	TaskType  string
	TaskIndex int
	WorkId    string
	TmpFiles  []string // 生成的临时文件名称
}

type NotifyFinishReply struct {
	Success bool
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
