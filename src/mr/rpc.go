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

const (
	_ int8 = iota
	MapPhase
	ReducePhase
	Wait
	Exit
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type MRRequest struct {
	TaskDone bool
	Phase int8
	Output string
}

type MRResponse struct {
	Task *Task
	ID int
}

type MapDone struct {
	MapID int
}

type Task struct {
	Phase int8
	MapFile string
	MapID int
	Nmap int
	ReduceID int
	NReduce int
	ReduceFiles []string
}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
