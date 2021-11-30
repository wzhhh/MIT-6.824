package kvraft

import (
	"labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

const (
	retryTime = 10 * time.Millisecond
)
// Clerk ...
/*
If the Clerk sends an RPC to the wrong kvserver, or if it cannot reach the kvserver,
	the Clerk should re-try by sending to a different kvserver.
If the key/value service commits the operation to its Raft log (and hence applies the operation to the key/value state machine),
	the leader reports the result to the Clerk by responding to its RPC.
If the operation failed to commit (for example, if the leader was replaced),
	the server reports an error, and the Clerk retries with a different server.
 */
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex
	clientId int64 // Clerk client self id
	seqId int64 // serial id of commands
	leaderId int // raft leader
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
	}
	_, _ = DPrintf("[C%d] request Get key: %s", ck.clientId, key)
	for {
		reply := GetReply{}
		if ck.servers[ck.currentLeader()].Call("KVServer.Get", &args, &reply) {
			if reply.Err == OK {
				_, _ = DPrintf("[C%d] response %s Get key: %s, val: %s", ck.clientId, reply.Err, key, reply.Value)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				_, _ = DPrintf("[C%d] response %s Get key: %s, val: %s", ck.clientId, reply.Err, key, reply.Value)
				return ""
			}
		}

		// timeout or not leader, server will reject and supply the most recent leader heard from AppendEntries
		ck.changeLeader()
		time.Sleep(retryTime)
	}
}

func (ck *Clerk) changeLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leaderId = (ck.leaderId+1)%len(ck.servers)
	return ck.leaderId
}

func (ck *Clerk) currentLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leaderId
}

func anotherServer(cur, total int) int {
	res := int(nrand()%int64(total))
	for res == cur {
		res = int(nrand()%int64(total))
	}
	return res
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		SeqId: atomic.AddInt64(&ck.seqId, 1),
	}
	_, _ = DPrintf("[C%d] request %s key: %s, val: %s, seq: %d", ck.clientId, op, key, value, args.SeqId)
	for {
		reply := PutAppendReply{}
		if ck.servers[ck.currentLeader()].Call("KVServer.PutAppend", &args, &reply) {
			if reply.Err == OK {
				_, _ = DPrintf("[C%d] response %s Ok key: %s, val: %s, seq: %d", ck.clientId, op, key, value, args.SeqId)
				return
			}
		}
		ck.changeLeader()
		time.Sleep(retryTime)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
