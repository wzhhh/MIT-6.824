package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	taskChan      chan *Task

	mapTasks map[int]*taskState
	mapLock 	sync.Mutex
	reduceTasks map[int]*taskState
	reduceLock sync.Mutex
	nMap int
	nReduce int
	mapDoneNum int
	reduceDoneNum int
	mapDoneChan chan struct{}

	workerID int
	workerLock sync.Mutex
}

type taskState struct {
	task   *Task
	timer  *time.Timer
	state int8
}

const (
	_ int8 = iota
	InProcessing
	Overtime
	Finish
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) Register(args *ExampleArgs, id *int) error {
	m.workerLock.Lock()
	defer m.workerLock.Unlock()
	*id = m.workerID
	m.workerID++
	return nil
}

func (m *Master) FinishMap(mapID *int, ack *bool) error {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	if m.mapTasks[*mapID].state != InProcessing {
		*ack = false
		return nil
	}
	m.mapTasks[*mapID].state = Finish
	m.mapTasks[*mapID].timer.Stop()
	m.mapDoneNum++
	log.Printf("[FinishMap] finish map task id: %d, finished: %d, all: %d\n", *mapID, m.mapDoneNum, m.nMap)
	if m.mapDoneNum == m.nMap {
		log.Printf("[FinishMap] finish all map tasks: %d\n", m.mapDoneNum)
		close(m.mapDoneChan)
	}
	*ack = true
	return nil
}

func (m *Master) FinishReduce(reduceID *int, ack *bool) error {
	m.reduceLock.Lock()
	defer m.reduceLock.Unlock()
	if m.reduceTasks[*reduceID].state != InProcessing {
		*ack = false
		return nil
	}
	m.reduceTasks[*reduceID].state = Finish
	m.reduceTasks[*reduceID].timer.Stop()
	m.reduceDoneNum++
	log.Printf("[FinishReduce] finish reduce task id: %d, finished: %d, all: %d\n", *reduceID, m.reduceDoneNum, m.nReduce)
	if m.reduceDoneNum == m.nReduce {
		log.Printf("[FinishReduce] finish all reduce tasks: %d\n", m.reduceDoneNum)
		close(m.taskChan)
	}
	*ack = true
	return nil
}

func (m *Master) GetTask(args *ExampleArgs, task *Task) error {
	select {
	case t, ok := <-m.taskChan:
		if !ok {
			log.Println("[GetTask] task chan closed, response exit")
			task.Phase = Exit
			return nil
		}
		switch t.Phase {
		case MapPhase:
			log.Println("[GetTask] get map task")
			m.mapLock.Lock()
			defer m.mapLock.Unlock()
			m.mapTasks[t.MapID] = &taskState{
				timer: time.NewTimer(10*time.Second),
				state: InProcessing,
				task: t,
			}
		case ReducePhase:
			log.Println("[GetTask] get reduce task")
			m.reduceLock.Lock()
			defer m.reduceLock.Unlock()
			m.reduceTasks[t.ReduceID] = &taskState{
				timer: time.NewTimer(10*time.Second),
				state: InProcessing,
				task: t,
			}
		}
		*task = *t
		return nil
	default:
		log.Println("[GetTask] no task get, response wait")
	}
	task.Phase = Wait
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	//ret := false

	// Your code here.
	if m.mapDoneNum < m.nMap {
		m.mapLock.Lock()
		for id := range m.mapTasks {
			if m.mapTasks[id].state == InProcessing {
				select {
				case <-m.mapTasks[id].timer.C:
					log.Printf("[Done] map task expired, id: %d\n", id)
					m.mapTasks[id].state = Overtime
					go func(id int) {
						log.Printf("[Done] repush map task, id: %d\n", id)
						m.taskChan <- m.mapTasks[id].task
					}(id)
				default:
				}
			}
		}
		m.mapLock.Unlock()
		return false
	}
	if m.reduceDoneNum < m.nReduce {
		m.reduceLock.Lock()
		for id := range m.reduceTasks {
			if m.reduceTasks[id].state == InProcessing {
				select {
				case <-m.reduceTasks[id].timer.C:
					log.Printf("[Done] reduce task expired, id: %d\n", id)
					m.reduceTasks[id].state = Overtime
					go func(id int) {
						log.Printf("[Done] repush reduce task, id: %d\n", id)
						m.taskChan <- m.reduceTasks[id].task
					}(id)
				}
			}
		}
		m.reduceLock.Unlock()
		return false
	}
	log.Printf("[Done] all tasks done")
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		taskChan:   make(chan *Task, 1),
		mapDoneChan: make(chan struct{}),
		mapTasks: map[int]*taskState{},
		reduceTasks: map[int]*taskState{},
		nMap: len(files),
		nReduce: nReduce,
	}

	// Your code here.
	go func() {
		go func() {
			for i, file := range files {
				log.Printf("[Master] push map task for file: %s, id: %d\n", file, i)
				task := &Task{
					Phase:   MapPhase,
					MapFile: file,
					MapID: i,
					Nmap: m.nMap,
					NReduce: m.nReduce,
				}
				m.taskChan <- task
			}
		}()

		<-m.mapDoneChan
		log.Printf("[Master] map taks done")

		for i := 0; i < m.nReduce; i++ {
			reduceFiles := make([]string, 0, m.nMap)
			for mapID := range files {
				reduceFiles = append(reduceFiles, Filename(mapID, i))
			}
			log.Printf("[Master] push reduce task for files: %v, id: %d\n", reduceFiles, i)
			task := &Task{
				Phase: ReducePhase,
				ReduceID: i,
				NReduce: m.nReduce,
				ReduceFiles: reduceFiles,
			}
			m.taskChan <- task
		}
		log.Println("[Master] exit task push")
	}()

	m.server()
	return &m
}

func Filename(mapID, reduceID int) string {
	return fmt.Sprintf("mr-%d-%d", mapID, reduceID)
}