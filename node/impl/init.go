package impl

import (
	chord "JDSys/node/chord/api"
	mongo "JDSys/node/mongo/api"
	"JDSys/node/mongo/communication"
	"JDSys/utils"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

/*
Esegue tutte le attività per rendere il nodo UP & Running
*/
func InitNode(node *Node) {
	utils.PrintHeaderL1("NODE SETUP")
	node.MongoClient = mongo.InitLocalSystem()
	InitHealthyNode(node)
	InitChordDHT(node)
	JoinChordDHT(node)
	InitListeningServices(node)
	InitRPCService(node)
	utils.PrintLineL1()
}

/*
Permette al nodo di essere rilevato come Healthy Instance dal Load Balancer e configura il DB locale
*/
func InitHealthyNode(node *Node) {
	utils.PrintHeaderL2("Initializing EC2 node")
	// Configura il sistema di storage locale
	node.MongoClient = mongo.InitLocalSystem()

	// Inizia a ricevere gli HeartBeat dal LB
	go StartHeartBeatListener()

	// Inizia a inviare valori poco acceduti su S3
	go node.MongoClient.CheckRarelyAccessed()

	// Attende di diventare healthy per il Load Balancer
	utils.PrintTs("Waiting for ELB Health Checking...")
	time.Sleep(utils.NODE_HEALTHY_TIME)
	utils.PrintTs("EC2 Node Up & Running!")
}

/*
Permette al nodo di entrare a far parte della DHT Chord in base alle informazioni ottenute dal Service Registry.
Inizia anche due routine per aggiornamento periodico delle FT del nodo stesso e degli altri nodi della rete
*/
func InitChordDHT(node *Node) {
	utils.PrintHeaderL2("Initializing Chord DHT")

	// Setup dei Flags
	addressPtr := flag.String("addr", "", "the port you will listen on for incomming messages")
	joinPtr := flag.String("join", "", "an address of a server in the Chord network to join to")
	flag.Parse()

	utils.PrintTs("Getting Local Outbound IP")
	// Ottiene l'indirizzo IP dell'host utilizzato nel VPC
	*addressPtr = utils.GetOutboundIP()
	node.ChordClient = new(chord.ChordNode)

	utils.PrintTs("Checking for active nodes in the chord ring")
	// Controlla le istanze attive contattando il Service Registry per entrare nella rete
waitLB:
	nodes := GetNodesDHT(utils.REGISTRY_IP)
	for {
		if len(nodes) == 0 {
			nodes = GetNodesDHT(utils.REGISTRY_IP)
		} else {
			break
		}
	}

	// Unica istanza attiva, se è il nodo stesso crea la DHT Chord, se non è lui
	// allora significa che non è ancora healthy per il LB e aspettiamo ad entrare nella rete
	if len(nodes) == 1 {
		if nodes[0] == *addressPtr {
			utils.PrintTs("No other nodes found. Creating Chord Ring")
			node.ChordClient = chord.Create(*addressPtr + utils.CHORD_PORT)
			first = true
		} else {
			goto waitLB
		}
	} else {
		// Se c'è più di un'istanza attiva viene contattato un altro nodo random per fare la Join
		utils.PrintTs("Joining Chord Ring")
		*joinPtr = nodes[rand.Intn(len(nodes))]
		for {
			if *joinPtr == *addressPtr {
				*joinPtr = nodes[rand.Intn(len(nodes))]
			} else {
				break
			}
		}
		node.ChordClient, _ = chord.Join(*addressPtr+utils.CHORD_PORT, *joinPtr+utils.CHORD_PORT)
		first = false
	}

	utils.PrintTs("My address is: " + *addressPtr)
	utils.PrintTs("Join address is: " + *joinPtr)

	// Se il nodo entra in un anello già esistente, attendiamo il corretto recupero di succ e pred prima di avviare il servizio
	if !first {
		utils.PrintTs("Updating successor and predecessor info...")
	retry:
		pred := node.ChordClient.GetPredecessor().GetIpAddr()
		if pred == "" {
			goto retry
		}
	}
	utils.PrintTs("Chord Node Started Succesfully!")
}

/*
Registra il servizio RPC, in modo che il nodo possa ricevere correttamente le chiamate RPC dal client e dagli altri nodi.
*/
func InitRPCService(node *Node) {
	utils.PrintHeaderL2("Starting RPC Service")

	srv := &http.Server{
		Addr:    utils.RPC_PORT,
		Handler: http.DefaultServeMux,
	}

	rpc.Register(node)
	rpc.HandleHTTP()

	utils.PrintTs("Start Serving RPC request on port " + utils.RPC_PORT)
	utils.PrintTs("RPC Service Correctly Started")
	go srv.ListenAndServe()
}

/*
Inizializza i servizi per il listening dei messaggi relativi a Replication e Reconciliation
*/
func InitListeningServices(node *Node) {
	recvMutex = new(sync.Mutex)
	sendMutex = new(sync.Mutex)
	node.Handler = false
	node.Round = 0

	utils.PrintHeaderL2("Starting Listening Services")
	go ListenReplicationMessages(node)
	go ListenReconciliationMessages(node)
	time.Sleep(1 * time.Millisecond)
}

/*
Permette al nodo di ottenere la lista degli altri nodi presenti nell'anello
*/
func GetNodesDHT(registryAddr string) []string {
	args := Args{}
	var reply []string

	client, _ := utils.HttpConnect(registryAddr, utils.REGISTRY_PORT)
	err := client.Call("DHThandler.GetActiveNodes", args, &reply)
	if err != nil {
		log.Fatal("RPC error: ", err)
	}
	return reply
}

/*
Finalizza il Join del Nodo nell'anello chord. Contatta il predecessore per richiedere l'invio delle sue entry.
*/
func JoinChordDHT(node *Node) {
	migrMutex = new(sync.Mutex)
	go ListenMigrationMessages(node)

	var reply string
	args := Args{}
	args.Value = node.ChordClient.GetIpAddress()

	utils.PrintHeaderL2("Asking Predecessor for his entries")
	if first {
		utils.PrintTs("First node of the ring, no predecessor!")
		return
	}

	pred := node.ChordClient.GetPredecessor().GetIpAddr()
	client, _ := utils.HttpConnect(pred, utils.RPC_PORT)
	err := client.Call("Node.JoinRPC", args, &reply)
	if err != nil {
		utils.PrintTs("JoinRPC error: " + err.Error())
		os.Exit(1)
	}
	utils.PrintTs(pred + ": " + reply)
}

/*
Resta in ascolto per messaggi di replicazione dagli altri nodi. Ad ogni messaggio viene aggiornato nello storage
locale l'informazione relativa all'entry ricevuta
*/
func ListenReplicationMessages(node *Node) {
	fileChannel := make(chan string)

	go communication.StartReceiver(fileChannel, recvMutex, utils.REPLN)
	utils.PrintTs("Started Update Message listening Service")
	for {
		received := <-fileChannel
		if received == "rcvd" {
			node.MongoClient.MergeCollection(utils.REPLICATION_EXPORT_FILE, utils.REPLICATION_RECEIVE_FILE)
			utils.ClearDir(utils.REPLICATION_RECEIVE_PATH)
			recvMutex.Unlock()
		}
	}
}

/*
Resta in ascolto per la ricezione dei messaggi di riconciliazione. Ogni volta che si riceve un messaggio vengono
risolti i conflitti aggiornando lo storage locale
*/
func ListenReconciliationMessages(node *Node) {
	fileChannel := make(chan string)

	go communication.StartReceiver(fileChannel, recvMutex, utils.RECON)
	utils.PrintTs("Started Reconciliation Message listening Service")
	for {
		// Si scrive sul canale per attivare la riconciliazione una volta ricevuto correttamente l'update dal predecessore
		received := <-fileChannel
		if received == "rcvd" {
			node.MongoClient.ReconciliateCollection(utils.RECONCILIATION_EXPORT_FILE, utils.RECONCILIATION_RECEIVE_FILE)
			utils.ClearDir(utils.RECONCILIATION_RECEIVE_PATH)
			recvMutex.Unlock()

			// Nodo non ha successore, aspettiamo la ricostruzione della DHT Chord finchè non viene
			// completato l'aggiornamento dell'anello
		retry:
			if node.ChordClient.GetSuccessor().String() == "" {
				utils.PrintTs("Node hasn't a successor, wait for the reconstruction...")
				time.Sleep(utils.WAIT_SUCC_TIME)
				goto retry
			}

			// Il nodo effettua export del DB e lo invia al successore
			addr := node.ChordClient.GetSuccessor().GetIpAddr()
			utils.PrintTs("DB forwarded to successor: " + addr)

			// Solamente per il nodo che ha iniziato l'aggiornamento incrementiamo il contatore che ci permette
			// di interrompere la riconciliazione dopo 2 giri non effettuando la SendUpdateMsg
			if node.Handler {
				node.Round++
				if node.Round == 2 {
					utils.PrintTs("Request returned to the node invoked by the registry two times, ring updated correctly")
					// Ripristiniamo le variabili per le future riconciliazioni
					node.Handler = false
					node.Round = 0
				} else {
					SendUpdateMsg(node, addr, utils.RECON, "")
				}
				// Se il nodo è uno di quelli intermedi, si limita a propagare il messaggio di riconciliazione
			} else {
				SendUpdateMsg(node, addr, utils.RECON, "")
			}
		}
	}
}

/*
Resta in ascolto per i messaggi di leave e join dagli altri nodi. Ad ogni messaggio si effettua il merge
delle entry ricevute con quelle presenti nello storage locale.
*/
func ListenMigrationMessages(node *Node) {
	fileChannel := make(chan string)

	go communication.StartReceiver(fileChannel, migrMutex, utils.MIGRN)
	utils.PrintTs("Started Migration listening Service")
	for {
		received := <-fileChannel
		if received == "rcvd" {
			node.MongoClient.MergeCollection(utils.MIGRATION_EXPORT_FILE, utils.MIGRATION_RECEIVE_FILE)
			utils.ClearDir(utils.MIGRATION_RECEIVE_PATH)
			migrMutex.Unlock()
		}
	}
}

/*
Inizializza un listener sulla porta 8888, su cui il Nodo riceve gli HeartBeat del Load Balancer.
*/
func StartHeartBeatListener() {
	utils.PrintTs("Start Listening Heartbeats from LB on port: " + utils.HEARTBEAT_PORT)
	http.HandleFunc("/", lb_handler)
	http.ListenAndServe(utils.HEARTBEAT_PORT, nil)
}
