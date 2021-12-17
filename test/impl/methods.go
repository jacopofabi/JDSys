package impl

import (
	"JDSys/utils"
	"net/rpc"
)

var WORKLOAD_GET []int
var WORKLOAD_PUT []int
var WORKLOAD_APP []int

var GET string = "Node.GetRPC"
var PUT string = "Node.PutRPC"
var DEL string = "Node.DeleteRPC"
var APP string = "Node.AppendRPC"

/*
Struttura che mantiene i parametri delle RPC
*/
type Args struct {
	Key     string
	Value   string
	Handler string
	Deleted bool
}

/*
Parametri per le operazioni di Get e Delete
*/
type Args1 struct {
	Key string
}

/*
Parametri per le operazioni di Put e Update
*/
type Args2 struct {
	Key   string
	Value string
}

/*
Esegue una operazione di Get per il testing
*/
func TestGet(key string, print bool, id int) {
	WORKLOAD_GET[id] = 1
	GetRPC(key, print)
	WORKLOAD_GET[id] = 0
}

/*
Esegue una operazione di Put per il testing
*/
func TestPut(key string, value string, print bool, id int) {
	WORKLOAD_PUT[id] = 1
	PutRPC(key, value, print)
	WORKLOAD_PUT[id] = 0

}

/*
Esegue una operazione di Append per il Workload
*/
func TestAppend(key string, value string, print bool, id int) {
	WORKLOAD_APP[id] = 1
	AppendRPC(key, value, print)
	WORKLOAD_APP[id] = 0

}

/*
Effettua la RPC per la GET di Workload
*/
func GetRPC(key string, print bool) {
	args := Args{}
	args.Key = key

	var reply *string

	client, _ := rpc.DialHTTP("tcp", utils.LB_DNS_NAME+utils.RPC_PORT)
	if client != nil {
		client.Call(GET, args, &reply)
		client.Close()
	}
}

/*
Effettua la RPC per il PUT di Workload
*/
func PutRPC(key string, value string, print bool) {
	args := Args{}
	args.Key = key
	args.Value = value

	var reply *string

	client, _ := rpc.DialHTTP("tcp", utils.LB_DNS_NAME+utils.RPC_PORT)
	if client != nil {
		client.Call(PUT, args, &reply)
		client.Close()
	}
}

/*
Effettua la RPC per l'APPEND di Workload
*/
func AppendRPC(key string, value string, print bool) {
	args := Args{}
	args.Key = key
	args.Value = value

	var reply *string
	client, _ := rpc.DialHTTP("tcp", utils.LB_DNS_NAME+utils.RPC_PORT)
	if client != nil {
		client.Call(APP, args, &reply)
		client.Close()
	}
}

/*
Effettua la RPC per il DELETE di Workload
*/
func DeleteRPC(key string, print bool) {
	args := Args{}
	args.Key = key

	var reply *string
	client, _ := rpc.DialHTTP("tcp", utils.LB_DNS_NAME+utils.RPC_PORT)
	if client != nil {
		client.Call(DEL, args, &reply)
		client.Close()
	}
}
