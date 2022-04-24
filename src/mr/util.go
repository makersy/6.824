package mr

import "fmt"

const (
	mTempFilePrefix = "mr-map-temp"
	mFilePrefix     = "mr-map"
	rTempFilePrefix = "mr-out-temp"
	rFilePrefix     = "mr-out"
)

// temp文件命名需要带上workerId：防止多个worker执行了同个任务，写文件出现冲突
func tempMapOutFile(workerId string, mTaskIdx, rTaskIdx int) string {
	return fmt.Sprintf("%v-%v-%v-%v", mTempFilePrefix, workerId, mTaskIdx, rTaskIdx)
}

func mapOutFile(mTaskIdx, rTaskIdx int) string {
	return fmt.Sprintf("%v-%v-%v", mFilePrefix, mTaskIdx, rTaskIdx)
}

func tempReduceOutFile(workerId string, rTaskIdx int) string {
	return fmt.Sprintf("%v-%v-%v", rTempFilePrefix, workerId, rTaskIdx)
}

func reduceOutFile(rTaskIdx int) string {
	return fmt.Sprintf("%v-%v", rFilePrefix, rTaskIdx)
}
