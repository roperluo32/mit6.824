package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		//map的任务数
		ntasks = len(mapFiles)
		//需要输出给reduce的文件数
		n_other = nReduce
	case reducePhase:
		//reduce的任务数
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	//1，将ntasks个任务分配给空闲的worker

	//1, 建立一个worker channel，存放空闲的worker
	ch := make(chan string, 2)

	//监听新注册worker routine的退出信号
	quitch := make(chan string)

	//2, 开启一个routine监听新注册的worker，并放入worker channel
	go func() {
		for {
			select {
			//将新注册的worker放入worker channel
			case w := <-registerChan:
				ch <- w
			case <-quitch:
				break
			}
		}
	}()

	var wg sync.WaitGroup
	taskcount := 0
	donecount := 0

	//3， 开始循环处理任务
	for i := 0; i < ntasks; i++ {
		//3.1 从worker channel中取出空闲的worker
		w := <-ch

		//3.2 组装发给worker的参数，包括文件名，是map还是reduce任务等信息
		args := new(DoTaskArgs)
		args.JobName = jobName
		args.File = mapFiles[i]
		args.Phase = phase
		args.TaskNumber = i
		args.NumOtherPhase = n_other

		wg.Add(1)
		taskcount += 1

		//3.3 向worker发送执行任务的请求
		go func() {
			ok := call(w, "Worker.DoTask", args, new(struct{}))
			//如果worker执行任务失败，那么重新从worker channel中取出一个worker请求执行，直到成功为止
			for ok == false {
				fmt.Printf("schedule: RPC %s to worker error\n", w)
				w = <-ch
				ok = call(w, "Worker.DoTask", args, new(struct{}))
			}

			//将worker放回worker channel中
			ch <- w

			defer func() {
				wg.Done()
			}()

		}()

	}

	//4， 等待所有worker请求执行完成
	wg.Wait()

	quitch <- "quit"
	fmt.Printf("Schedule: %v phase done\n", phase)
}
