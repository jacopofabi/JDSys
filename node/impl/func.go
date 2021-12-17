package impl

import (
	"JDSys/node/mongo/communication"
	"JDSys/utils"
	"fmt"
	"net/http"
	"sync"
	"time"
)

var first bool
var sendMutex *sync.Mutex
var recvMutex *sync.Mutex
var migrMutex *sync.Mutex

/*
Invia un messaggio di aggiornamento ad un nodo remoto. Con 'mode' si specifica il tipo di messaggio tra
Reconciliation/Migration/Replication. Viene quindi esportato il file CSV dello storage locale e lo si invia al nodo remoto.
*/
func SendUpdateMsg(node *Node, address string, mode string, key string) error {
	var file string
	var path string
	var err error

	sendMutex.Lock()

	switch mode {
	case utils.REPLN:
		utils.PrintHeaderL2("Sending replica to successor " + address + " via TCP")
		file = utils.REPLICATION_SEND_FILE
		path = utils.REPLICATION_SEND_PATH
		err = node.MongoClient.ExportDocument(key, file)
	case utils.RECON:
		utils.PrintHeaderL2("Sending reconciliation message to successor " + address + " via TCP")
		file = utils.RECONCILIATION_SEND_FILE
		path = utils.RECONCILIATION_SEND_PATH
		err = node.MongoClient.ExportCollection(file)
	case utils.MIGRN:
		utils.PrintHeaderL3("Sending migration entries to: " + address)
		file = utils.MIGRATION_SEND_FILE
		path = utils.MIGRATION_SEND_PATH
		err = node.MongoClient.ExportCollection(file)
	}

	if err != nil {
		utils.ClearDir(path)
		utils.PrintTs("File not exported. Message not sent.")
		sendMutex.Unlock()
		return err
	}

	err = communication.StartSender(file, address, mode)

	if err != nil {
		utils.ClearDir(path)
		utils.PrintTs("Message not sent.")
		sendMutex.Unlock()
		return err
	}

	utils.ClearDir(path)
	utils.PrintTs("Message sent correctly.")
	sendMutex.Unlock()
	return nil
}

/*
Permette ad un nodo di inviare la un'entry al suo successore per la replicazione
*/
func SendReplicaToSuccessor(node *Node, key string) {
	succ := node.ChordClient.GetSuccessor().GetIpAddr()
	if succ != "" {
		err := SendUpdateMsg(node, succ, utils.REPLN, key)
		if err != nil {
			return
		}
		utils.PrintTs("Replica sent Correctly")
	} else {
		utils.PrintTs("Node hasn't a successor yet, data will be replicated later")
	}
}

/*
Permette di propagare la richiesta di Delete a tutti i nodi, cancellando quindi anche le repliche presenti sull'anello
*/
func DeleteReplicas(node *Node, args *Args, reply *string) {
	utils.PrintTs("Forwarding delete request")
retry:
	succ := node.ChordClient.GetSuccessor().GetIpAddr()
	if succ == "" {
		utils.PrintTs("Node hasn't a successor yet, replicas will be deleted later")
		time.Sleep(utils.WAIT_SUCC_TIME)
		goto retry
	}
	client, _ := utils.HttpConnect(succ, utils.RPC_PORT)
	utils.PrintTs("Delete request forwarded to replication node: " + succ + utils.RPC_PORT)
	client.Call("Node.DeleteReplicating", args, &reply)
}

/*
Gestisce gli hearthbeat del Load Balancer ed i messaggi di Terminazione dal Service Registry
*/
func lb_handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "JDSys Key-Value Storage")
}
