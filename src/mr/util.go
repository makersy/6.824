package mr

import "fmt"

const (
	mTempFilePrefix = "mr-mapout-temp"
	mFilePrefix     = "mr-mapout"
	rTempFilePrefix = "mr-out-temp"
	rFilePrefix     = "mr-out"
)

// temp文件命名需要带上workerId：防止多个worker执行了同个任务，写文件出现冲突
func tempMOutFileName(workerId string) string {
	return fmt.Sprintf("%v-%v-*", mTempFilePrefix, workerId)
}

func mOutFileName(mTaskIdx, rTaskIdx int) string {
	return fmt.Sprintf("%v-%v-%v", mFilePrefix, mTaskIdx, rTaskIdx)
}

func tempROutFile(workerId string) string {
	return fmt.Sprintf("%v-%v-*", rTempFilePrefix, workerId)
}

func rOutFileName(rTaskIdx int) string {
	return fmt.Sprintf("%v-%v", rFilePrefix, rTaskIdx)
}
