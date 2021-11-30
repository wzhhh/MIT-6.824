package kvraft

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Debug = 0

	serverTTL = 2000 * time.Millisecond
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Index    int    // log index
	Term     int    // log term
	Type     string // PutAppend, Get
	Key      string
	Value    string
	SeqId    int64
	ClientId int64
}

// Op context used to wake locked RPC in waiting Raft to commit
type OpContext struct {
	op       Op
	commitCh chan struct{} // notify channel once committed

	wrongLeader bool // index log term not identical
	stale       bool // seqId smaller

	keyExist bool // for get
	value    string
}

func newOpContext(op Op) *OpContext {
	opCtx := &OpContext{
		op:       op,
		commitCh: make(chan struct{}),
	}
	return opCtx
}

type KVServer struct {
	mu      sync.Mutex
	me      int // index of the current server in servers[]
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore map[string]string
	reqMap  map[int]*OpContext // log index -> context
	seqMap  map[int64]int64    // clientId -> segId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK
	op := Op{
		Type:     TypeGet,
		Key:      args.Key,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
	}

	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	opCtx := newOpContext(op)
	kv.mu.Lock()
	kv.reqMap[op.Index] = opCtx
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if one, ok := kv.reqMap[op.Index]; ok {
			if one == opCtx {
				delete(kv.reqMap, op.Index)
			}
		}
		kv.mu.Unlock()
	}()

	select {
	case <-opCtx.commitCh:
		if opCtx.wrongLeader {
			reply.Err = ErrWrongLeader
		} else if !opCtx.keyExist {
			reply.Err = ErrNoKey
		} else {
			reply.Value = opCtx.value
		}
	case <-time.After(serverTTL):
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer func() {
		DPrintf("[S%d] PutAppend reply: %s", kv.me, reply.Err)
	}()
	reply.Err = OK

	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
	}

	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if !isLeader {
		DPrintf("[S%d] PutAppend start not leader", kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[S%d] PutAppend op: %#v", kv.me, op)
	opCtx := newOpContext(op)
	kv.mu.Lock()
	kv.reqMap[op.Index] = opCtx
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if one, ok := kv.reqMap[op.Index]; ok {
			if one == opCtx {
				delete(kv.reqMap, op.Index)
			}
		}
		kv.mu.Unlock()
	}()

	select {
	case <-opCtx.commitCh:
		if opCtx.wrongLeader {
			DPrintf("[S%d] PutAppend commit wrong leader", kv.me)
			reply.Err = ErrWrongLeader
		} else if opCtx.stale {
			// do nothiing
		}
	case <-time.After(serverTTL):
		DPrintf("[S%d] PutAppend timeout", kv.me)
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) notifyCommit() {
	for !kv.killed() {
		for msg := range kv.applyCh {

			//_, _ = DPrintf("[notifyCommit] [S%d] receive commit msg %v", kv.me, msg)
			func (){
				cmd := msg.Command
				index := msg.CommandIndex
				kv.mu.Lock()
				defer kv.mu.Unlock()

				op, ok := cmd.(Op)
				if !ok {
					_, _ = DPrintf("[notifyCommit] [S%d] wrong type(%v) msg %v", kv.me, reflect.TypeOf(cmd).String(), msg)
					return
				}
				//opCtx, existOp := kv.reqMap[op.Index]
				opCtx, existOp := kv.reqMap[index]
				prevSeq, existSeq := kv.seqMap[op.ClientId]

				//DPrintf("[------------notifyCommit S%d] op: %#v, opCtx: %#v, seqMap: %v", kv.me, op, opCtx, kv.seqMap)

				if prevSeq < op.SeqId {
					kv.seqMap[op.ClientId] = op.SeqId
				}

				defer func() {
					if existOp {
						close(kv.reqMap[index].commitCh)
					}
				}()

				if existOp {
					if opCtx.op.Term != msg.CommandTerm {
						kv.reqMap[index].wrongLeader = true
					}
				}

				switch op.Type {
				case TypeGet:
					if existOp {
						//opCtx.keyExist = true
						//opCtx.value = kv.kvStore[op.Key]

						kv.reqMap[index].value, kv.reqMap[index].keyExist = kv.kvStore[op.Key]
					}
				case TypePut, TypeAppend:
					//DPrintf("[notifyCommit S%d] put exist seq: %v op.seq: %v, prevSeq: %v, existOp: %v, key-val: %s-%s",
					//	kv.me, existSeq, op.SeqId, prevSeq, existOp, op.Key, op.Value)
					if !existSeq || op.SeqId > prevSeq {
						if op.Type == TypePut {
							kv.kvStore[op.Key] = op.Value
						} else if op.Type == TypeAppend {
							if val, exist := kv.kvStore[op.Key]; exist {
								kv.kvStore[op.Key] = val+op.Value
							} else {
								kv.kvStore[op.Key] = op.Value
							}
						}
					} else if existOp {
						kv.reqMap[index].stale = true
					}
				}
				_, _ = DPrintf("[notifyCommit] [S%d] %6s kvStore[%v], %v, opCtx: %#v",
					kv.me, op.Type, kv.kvStore, reflect.TypeOf(cmd).String(), kv.reqMap[index])
			}()
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = map[string]string{}
	kv.reqMap = map[int]*OpContext{}
	kv.seqMap = map[int64]int64{}

	go kv.notifyCommit()

	return kv
}
