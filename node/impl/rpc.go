package impl

import (
	chord "JDSys/node/chord/api"
	mongo "JDSys/node/mongo/api"
	"JDSys/utils"
	"fmt"
	"time"
)

/*
Struttura che mantiene tutte le informazioni di un nodo
*/
type Node struct {
	MongoClient mongo.MongoInstance
	ChordClient *chord.ChordNode

	// Variabili per la realizzazione della consistenza finale
	Handler bool
	Round   int
}

/*
Struttura che mantiene i parametri utilizzati nelle RPC
*/
type Args struct {
	Key     string
	Value   string
	Handler string
	Deleted bool
}

/*
Effettua la RPC per la Get di una Key.
 1) Si verifica se il nodo ha una copia della risorsa
 2) Lookup per trovare il nodo che hosta la risorsa
 3) RPC effettiva di GET verso quel nodo chord
*/
func (n *Node) GetRPC(args *Args, reply *string) error {
	utils.PrintHeaderL2("Received Get RPC for key " + args.Key)
	utils.PrintTs("Checking value on local storage")
	entry := n.MongoClient.GetEntry(args.Key)
	if entry != nil {
		*reply = entry.FormatClient()
		utils.PrintTs("Generating RPC Reply:")
		fmt.Println(*reply)
		utils.PrintTs("Finished. Replying to caller")
		return nil
	} else {
		utils.PrintTs("Key not found on local storage.")
	}

	// senza successore non possiamo propagare la richiesta, il nodo potrebbe essere da solo e la chiave non c'è realmente,
	// oppure il gestore della chiave è un altro, quindi il client è costretto a riprovare in attesa che si ricostruisca
	// l'anello per recuperare effettivamente il valore associato alla chiave
	succ := n.ChordClient.GetSuccessor().GetIpAddr()
	if succ == "" && entry == nil {
		*reply = "Key not found."
		return nil
	}

	utils.PrintTs("Forwarding Get Request on Handling Node")
	addr, _ := chord.Lookup(utils.HashString(args.Key), succ+utils.CHORD_PORT)
	client, _ := utils.HttpConnect(utils.RemovePort(addr), utils.RPC_PORT)
	utils.PrintTs("Request sent to: " + utils.ParseAddrRPC(addr))
	client.Call("Node.GetImpl", args, &reply)
	return nil
}

/*
Effettua la RPC per inserire un'entry nello storage.
 1) Lookup per trovare il nodo che deve gestire la risorsa
 2) RPC effettiva di PUT verso quel nodo chord
*/
func (n *Node) PutRPC(args Args, reply *string) error {
	utils.PrintHeaderL2("Received Put RPC for key " + args.Key)

	me := n.ChordClient.GetIpAddress()
	addr, _ := chord.Lookup(utils.HashString(args.Key), me+utils.CHORD_PORT)
	client, _ := utils.HttpConnect(utils.RemovePort(addr), utils.RPC_PORT)
	utils.PrintTs("Checking Key Handling")
	utils.PrintTs("Request sent to: " + utils.ParseAddrRPC(addr))

	client.Call("Node.PutImpl", args, &reply)
	return nil
}

/*
Effettua la RPC per aggiornare un'entry nello storage.
 1) Lookup per trovare il nodo che hosta la risorsa
 2) RPC effettiva di APPEND verso quel nodo chord
*/
func (n *Node) AppendRPC(args Args, reply *string) error {
	utils.PrintHeaderL2("Received Append RPC for key " + args.Key)

	me := n.ChordClient.GetIpAddress()
	addr, _ := chord.Lookup(utils.HashString(args.Key), me+utils.CHORD_PORT)
	client, _ := utils.HttpConnect(utils.RemovePort(addr), utils.RPC_PORT)

	utils.PrintTs("Checking Key Handling")
	utils.PrintTs("Request sent to: " + utils.ParseAddrRPC(addr))
	client.Call("Node.AppendImpl", args, &reply)
	return nil
}

/*
Effettua la RPC per eliminare un'entry nello storage.
 1) Lookup per trovare il nodo che hosta la risorsa
 2) RPC effettiva di DELETE verso quel nodo chord
 3) La delete viene inoltrata su tutto l'anello
*/
func (n *Node) DeleteRPC(args Args, reply *string) error {
	utils.PrintHeaderL2("Received Delete RPC for key " + args.Key)

	me := n.ChordClient.GetIpAddress()
	handlerNode, _ := chord.Lookup(utils.HashString(args.Key), me+utils.CHORD_PORT)
	args.Handler = utils.RemovePort(handlerNode)
	args.Deleted = false

	client, _ := utils.HttpConnect(utils.RemovePort(handlerNode), utils.RPC_PORT)
	utils.PrintTs("Checking Key Handling")
	utils.PrintTs("Delete request forwarded to handling node: " + utils.ParseAddrRPC(handlerNode))
	client.Call("Node.DeleteHandling", args, &reply)
	return nil
}

/*
Effettua il get. Scrive in reply la stringa contenente l'entry richiesta. Se l'entry
non è stata trovata restituisce un messaggio di errore.
*/
func (n *Node) GetImpl(args Args, reply *string) error {
	utils.PrintHeaderL2("Received Get RPC for key " + args.Key)
	utils.PrintTs("I'm the handling node")
	entry := n.MongoClient.GetEntry(args.Key)
	if entry == nil {
		*reply = "Entry not found"
	} else {
		*reply = entry.FormatClient()
	}
	utils.PrintTs("Generating RPC Reply:")
	fmt.Println(*reply)
	utils.PrintTs("Finished. Replying to caller")
	return nil
}

/*
Effettua il PUT. Ritorna 0 se l'operazione è avvenuta con successo, altrimenti l'errore specifico
*/
func (n *Node) PutImpl(args Args, reply *string) error {
	utils.PrintHeaderL2("Received Put RPC for key " + args.Key)
	utils.PrintTs("I'm the handling node")
	arg1 := args.Key
	arg2 := args.Value
	err := n.MongoClient.PutEntry(arg1, arg2)
	ok := true
	if err == nil {
		*reply = "Entry correctly inserted in the DB"
	} else if err.Error() == "Updated" {
		*reply = "Entry already exists. Correctly updated"
	} else {
		*reply = err.Error()
		ok = false
	}
	utils.PrintTs("Generating RPC Reply:")
	fmt.Println(*reply)
	utils.PrintTs("Finished. Replying to caller")

	// Inserimento avvenuto correttamente, procediamo con l'invio della replica al successore
	if ok {
		go SendReplicaToSuccessor(n, args.Key)
	}
	return nil
}

/*
Effettua l'APPEND. Ritorna 0 se l'operazione è avvenuta con successo, altrimenti l'errore specifico
*/
func (n *Node) AppendImpl(args *Args, reply *string) error {
	utils.PrintHeaderL2("Received Append RPC for key " + args.Key)
	utils.PrintTs("I'm the handling node")
	arg1 := args.Key
	arg2 := args.Value
	err := n.MongoClient.AppendValue(arg1, arg2)
	ok := true
	if err == nil {
		*reply = "Value correctly appended"
	} else {
		*reply = "Entry not found"
		ok = false
	}
	utils.PrintTs("Generating RPC Reply:")
	fmt.Println(*reply)
	utils.PrintTs("Finished. Replying to caller")

	// Inserimento avvenuto correttamente, procediamo con l'invio della replica al successore
	if ok {
		go SendReplicaToSuccessor(n, args.Key)
	}
	return nil
}

/*
Effettua il delete della risorsa sul nodo che deve gestirla.
Ritorna 0 se l'operazione è avvenuta con successo, altrimenti l'errore specifico
*/
func (n *Node) DeleteHandling(args *Args, reply *string) error {
	utils.PrintHeaderL2("Received Delete RPC for key " + args.Key)
	utils.PrintTs("I'm the handling node")
	utils.PrintTs("Deleting value on local storage")
	err := n.MongoClient.DeleteEntry(args.Key)
	if err == nil {
		args.Deleted = true
		*reply = "Entry successfully deleted"
	} else {
		// Entry non è presente nel DB del nodo gestore, quindi non esiste
		if err.Error() == "EntryNotFound" {
			*reply = "The key searched for deletion does not exist"
			utils.PrintTs(*reply)
			return nil
		}
	}
	utils.PrintTs(*reply)

	// Se l'entry esiste ed è stata cancellata, procediamo inoltrando la richiesta al nodo successore
	// così da eliminare tutte le repliche nell'anello
	go DeleteReplicas(n, args, reply)
	return nil
}

/*
Effettua il delete della risorsa replicata.
Ritorna 0 se l'operazione è avvenuta con successo, altrimenti l'errore specifico
*/
func (n *Node) DeleteReplicating(args *Args, reply *string) error {

	// La richiesta ha completato il giro dell'anello se è tornata al nodo che gestisce quella chiave
	if n.ChordClient.GetIpAddress() == args.Handler {
		utils.PrintTs("Delete Request returned to the handling node")
		if args.Deleted {
			*reply = "Entry succesfully deleted"
		} else {
			*reply = "Entry to delete not found"
		}
		utils.PrintTs(*reply)
		return nil
	}
	utils.PrintHeaderL2("Received Delete RPC for key " + args.Key)

	// Cancella l'entry richiesta sul db locale
	utils.PrintTs("Deleting replicated value on local storage")
	n.MongoClient.DeleteEntry(args.Key)

	// Propaga la Delete al nodo successivo, la cancellazione sul nodo che gestisce la chiave
	// è già stata effettuata, per questo se i nodi successivi non hanno successore aspettiamo
	// la ricostruzione della DHT Chord finchè non viene completata la Delete!
	utils.PrintTs("Forwarding delete request")
retry:
	succ := n.ChordClient.GetSuccessor().GetIpAddr()
	if succ == "" {
		utils.PrintTs("Node hasn't a successor, wait for the reconstruction...")
		time.Sleep(utils.WAIT_SUCC_TIME)
		goto retry
	}
	client, _ := utils.HttpConnect(succ, utils.RPC_PORT)
	utils.PrintTs("Delete request forwarded to replication node: " + succ + utils.RPC_PORT)
	client.Call("Node.DeleteReplicating", args, &reply)
	return nil
}

/*
Metodo invocato dal Service Registry quando le istanze EC2 devono procedere con lo scambio degli aggiornamenti
Effettua il trasferimento del proprio DB al nodo successore nella rete per realizzare la consistenza finale.
*/
func (n *Node) StartReconciliationRPC(args *Args, reply *string) error {
	utils.PrintHeaderL2("Reconciliation requested by service registry")

	succ := n.ChordClient.GetSuccessor().GetIpAddr()
	if succ == "" {
		*reply = "Node hasn't a successor, abort and wait for the reconstruction of the DHT."
		utils.PrintTs(*reply)
		return nil
	}

	// Imposto il nodo corrente come gestore dell'aggiornamento dell'anello, così da incrementare solo
	// per lui il contatore che permette l'interruzione dopo 2 giri
	n.Handler = true

	// Effettuo l' export del DB e lo invio al successore
	SendUpdateMsg(n, succ, utils.RECON, "")
	return nil
}

/*
Metodo invocato dal Service Registry quando l'istanza EC2 viene schedulata per la terminazione
Effettua il trasferimento del proprio DB al nodo successore nella rete per garantire persistenza dei dati.
Inviamo tutto il DB e non solo le entry gestite dal preciso nodo così abbiamo la possibilità di
aggiornare altri dati obsoleti mantenuti dal successore.
*/
func (n *Node) LeaveRPC(args *Args, reply *string) error {
	utils.PrintHeaderL2("Node Leaving")
	utils.PrintTs("Instance Scheduled to Terminating")
	utils.PrintTs("Sending entries to successor")
retry:
	succ := n.ChordClient.GetSuccessor().GetIpAddr()
	if succ == "" {
		utils.PrintTs("Node hasn't a successor, wait for the reconstruction of the DHT")
		time.Sleep(utils.WAIT_SUCC_TIME)
		goto retry
	}

	SendUpdateMsg(n, succ, utils.MIGRN, "")
	*reply = "Instance can now safely leave the chord ring"
	utils.PrintTs(*reply)
	return nil
}

/*
Metodo invocato dal nodo successore quando si inserisce nell'anello chord.
Effettua il trasferimento del proprio DB al chiamante in modo da permettere da subito le complete funzionalità del nuovo nodo.
Inviamo tutto il DB e non solo le entry gestite dal preciso nodo in modo da inviare anche eventuali repliche che questo dovrà gestire.
*/
func (n *Node) JoinRPC(args *Args, reply *string) error {
	succ := args.Value
	utils.PrintHeaderL2("Node Joining")
	utils.PrintTs("Instance is joining chord DHT")
	utils.PrintTs("Sending entries to new successor node")

retry:
	if succ == "" {
		utils.PrintTs("Node hasn't a successor, wait for the reconstruction of the DHT")
		time.Sleep(utils.WAIT_SUCC_TIME)
		goto retry
	}

	SendUpdateMsg(n, succ, utils.MIGRN, "")
	*reply = "Instance succesfully inserted in chord ring"
	utils.PrintTs(*reply)
	return nil
}
