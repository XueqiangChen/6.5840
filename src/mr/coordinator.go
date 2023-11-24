package mr

import (
	"log"
	"os"
)
import "net"
import "net/rpc"
import "net/http"

type Task struct {
	Type      string     // 任务类型：map，reduce
	Status    string     // 存储每一个Map和Reduce任务的状态：空闲，工作中，完成
	FileInfos []FileInfo // 文件信息
}

type FileInfo struct {
	Type     string //输入、输出
	Size     int    //文件大小
	Location string //存储位置
}

type Resource struct {
	CPU    float64
	Memory float64
}

type WorkerInfo struct {
	Id      int64
	Status  string //online，offline
	HeaBeat int64  // 存心跳时间，存个时间戳,每10s检查一次
	Resource
}

type Coordinator struct {
	// Your definitions here.
	// 任务列表
	Tasks []Task
	// worker的信息
	Workers []WorkerInfo

	NReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	//
	//reply.Y = args.X + 1
	log.Printf("Request args: %v\n", args)
	// check Worker info
	workerInfo := args.WorkerInfo
	if len(c.Workers) == 0 {
		c.Workers = append(c.Workers, workerInfo)
	} else {
		for _, worker := range c.Workers {
			if worker.Id != workerInfo.Id {
				c.Workers = append(c.Workers, workerInfo)
			} else {
				// update worker info
			}
		}
	}

	// 根据资源调度任务

	// 如果请求参数里面有task的信息，就把task的信息拿出来更新内存，否则
	// 将分配的任务调度到新机器上
	reply.Tasks = c.Tasks

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

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Println("Starting Coordinator......")
	c := Coordinator{}

	// Your code here.
	// 切分文件，确定map的个数
	i := 0
	var tasks []Task
	for i < len(files) {
		task := Task{
			Type:   "Map",
			Status: "INIT",
		}

		var fileInfos []FileInfo
		fileInfo := FileInfo{
			Type:     "Input",
			Size:     0,
			Location: files[i],
		}
		fileInfos = append(fileInfos, fileInfo)
		task.FileInfos = fileInfos
		tasks = append(tasks, task)
	}
	c.Tasks = tasks

	// todo
	c.NReduce = nReduce

	c.server()
	return &c
}
