package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	task := Task{}
	args := ExampleArgs{}
	id := 0
	ok := call("Master.Register", &args, &id)
	if !ok {
		return
	}
	log.Printf("[Worker %d] start\n", id)
	for {
		task = Task{}
		ok := call("Master.GetTask", &args, &task)
		if !ok {
			log.Printf("[Worker %d] GetTask not response, exit\n", id)
			return
		}
		switch task.Phase {
		case MapPhase:
			log.Printf("[Worker %d] map %#v", id, task)
			_ = doMap(&task, mapf)
			mapID, ack := task.MapID, false
			ok := call("Master.FinishMap", &mapID, &ack)
			if !ok {
				log.Printf("[Worker %d] FinishMap not response, task: %d\n", id, mapID)
				return
			}
			if ack {
				log.Printf("[Worker %d] finish map task: %d\n", id, mapID)
				//rename("map", files)
			} else {
				log.Printf("[Worker %d] finish map not ack\n", id)
			}
		case ReducePhase:
			log.Printf("[Worker %d] reduce %#v", id, task)
			_ = doReduce(&task, reducef)
			reduceID, ack := task.ReduceID, false
			ok := call("Master.FinishReduce", &reduceID, &ack)
			if !ok {
				log.Printf("[Worker %d] FinishReduce not response, task: %d\n", id, reduceID)
				return
			}
			if ack {
				log.Printf("[Worker %d] finish reduce task: %d\n", id, reduceID)
				//rename("reduce", [][2]string{file})
			} else {
				log.Printf("[Worker %d] finish reduce not ack\n", id)
			}
		case Wait:
			log.Printf("[Worker %d] wait\n", id)
			time.Sleep(5*time.Second)
		case Exit:
			log.Printf("[Worker %d] exit\n", id)
			return
		default:
		}
	}

	// uncomment to send the Example RPC to the master.
	//CallExample()

}

func rename(phase string, files [][2]string) {
	log.Printf("[rename] rename files, phase: %s\n", phase)
	for _, file := range files {
		err := os.Rename(file[0], file[1])
		if err != nil {
			log.Fatalf("cannot rename from %s to %s", file[0], file[1])
		}
	}
	log.Printf("[rename] rename files done, phase: %s\n", phase)
}

func doMap(task *Task, mapf func(string, string) []KeyValue) [][2]string {
	file, err := os.Open(task.MapFile)
	if err != nil {
		log.Fatalf("cannot open %s", task.MapFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.MapFile)
	}
	file.Close()
	kva := mapf(task.MapFile, string(content))

	outChs := make([]chan KeyValue, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		outChs[i] = make(chan KeyValue, 1)
	}
	go func() {
		defer func() {
			for i := range outChs {
				close(outChs[i])
			}
		}()
		for i := range kva {
			reduceID := ihash(kva[i].Key)%task.NReduce
			outChs[reduceID] <- kva[i]
		}
	}()

	fileChan := make(chan [2]string, 1)
	var wg sync.WaitGroup
	for i := range outChs {
		wg.Add(1)
		go func(reduceID int) {
			defer wg.Done()
			trueName := Filename(task.MapID, reduceID)
			file, err := os.CreateTemp("", trueName)
			if err != nil {
				log.Fatalf("cannot create temp %s", file.Name())
				return
			}
			//log.Printf("[doMap] create temp file %s\n", file.Name())
			defer func() {
				_ = file.Close()
				_ = os.Rename(file.Name(), trueName)
			}()
			enc := json.NewEncoder(file)
			for kv := range outChs[reduceID] {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("connot encode kv %v", kv)
					return
				}
			}
			fileChan <- [2]string{file.Name(), trueName}
		}(i)
	}

	go func() {
		wg.Wait()
		close(fileChan)
	}()

	var files [][2]string
	for file := range fileChan {
		files = append(files, file)
	}
	return files
}

func doReduce(task *Task, reducef func(string, []string) string) [2]string {
	kvChan := make(chan KeyValue, len(task.ReduceFiles))
	var wg sync.WaitGroup
	for _, filename := range task.ReduceFiles{
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			defer func() {
				_ = file.Close()
			}()
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kvChan <- kv
			}
		}(filename)
	}

	go func() {
		wg.Wait()
		close(kvChan)
	}()

	kvMap := map[string][]string{}
	for kv := range kvChan {
		if _, ok := kvMap[kv.Key]; !ok {
			kvMap[kv.Key] = []string{}
		}
		kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
	}

	oname := fmt.Sprintf("mr-out-%d", task.ReduceID)
	ofile, _ := os.CreateTemp("", oname)
	defer func() {
		_ = ofile.Close()
		_ = os.Rename(ofile.Name(), oname)
	}()
	//log.Printf("[doReduce] create temp file %s\n", ofile.Name())

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	for key, values := range kvMap {
		output := reducef(key, values)
		_, _ = fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
	return [2]string{ofile.Name(), oname}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
