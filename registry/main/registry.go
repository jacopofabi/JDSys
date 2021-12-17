package main

import (
	"JDSys/registry/amazon"
	"JDSys/utils"
	"context"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
	"time"
)

/*
Struttura per il passaggio dei parametri alla RPC
*/
type Args struct {
	Handler string
	Deleted bool
}

/*
Servizio per le RPC del registry
*/
type DHThandler int

func main() {
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	utils.ClearScreen()
	server := InitRegistry()

	//Aspetta segnali per chiudere tutte le connessioni al Ctrl+C
	<-done
	utils.PrintTs("Server Stopped")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		utils.PrintTs("Server Shutdown Failed: " + err.Error())
		os.Exit(1)
	}
	utils.PrintTs("Server Exited Properly")
}

/*
Fornisce la lista dei nodi presenti nell'anello ad un nuovo nodo che vuole effettuare il join
*/
func (s *DHThandler) GetActiveNodes(args *Args, reply *[]string) error {
	instances := checkActiveNodes()
	var list = make([]string, len(instances))
	for i := 0; i < len(instances); i++ {
		list[i] = instances[i].PrivateIP
	}
	*reply = list
	return nil
}

/*
Inizializza il servizio DHT
*/
func InitializeDHTService() *DHThandler {
	service := new(DHThandler)
	return service
}

/*
Restituisce tutte le istanze healthy presenti
*/
func checkActiveNodes() []amazon.InstanceEC2 {
	instances := amazon.GetActiveNodes()
	return instances
}

/*
Controlla periodicamente quali sono le istanze in terminazione. Invia a queste un segnale in modo che prima
di terminare possano inviare le proprie entry ad un altro nodo
*/
func StartCheckTerminatingNodes() {
	utils.PrintHeaderL2("Starting Checking Terminating Nodes")
	go amazon.Start_cache_flush_service()
	for {
		terminating := amazon.GetTerminatingInstances()
		for _, t := range terminating {
			sendTerminatingSignalRPC(t.PrivateIP)
		}
		time.Sleep(utils.CHECK_TERMINATING_INTERVAL)
	}
}

/*
Invoca la RPC che invia il segnale di terminazione ad un nodo schedulato per la terminazione
*/
func sendTerminatingSignalRPC(ip string) {
	utils.PrintTs("Sending Terminating Message to node: " + ip)
	client, _ := utils.HttpConnect(ip, utils.RPC_PORT)
	var reply string
	args := Args{}
	err := client.Call("Node.LeaveRPC", args, &reply)
	if err != nil {
		utils.PrintTs("LeaveRPC error: " + err.Error())
		os.Exit(1)
	}
	utils.PrintTs(ip + ": " + reply)
}

/*
Avvia periodicamente il processo iterativo di scambio di aggiornamenti tra un nodo e il suo successore per la riconciliazione.
Il processo permette di raggiungere la consistenza finale se non si verificano aggiornamenti in questa finestra temporale
*/
func StartPeriodicReconciliation() {
	utils.PrintHeaderL2("Starting periodic updates for reconciliation Routine")
	for {
		time.Sleep(utils.START_CONSISTENCY_INTERVAL)
	retry:
		nodes := checkActiveNodes()
		if len(nodes) == 0 || len(nodes) == 1 {
			time.Sleep(utils.WAIT_SUCC_TIME)
			goto retry
		}
		// Recuperate tutte le istanze attive, si invia la richiesta ad un nodo a caso
		var list = make([]string, len(nodes))
		for i := 0; i < len(nodes); i++ {
			list[i] = nodes[i].PrivateIP
		}
		utils.PrintHeaderL3("Reconciliation Routine")
		utils.PrintTs("Choosing random node to start the reconciliation")
		startReconciliationRPC(list[rand.Intn(len(list))])
	}
}

/*
Invocazione dell'RPC che avvia lo scambio di aggiornamenti tra i nodi per raggiungere la consistenza finale
*/
func startReconciliationRPC(ip string) {
	var reply string
	args := Args{}
	args.Handler = ""
	args.Deleted = false

	utils.PrintTs("Sending db exchange signal to node: " + ip)
	client, _ := utils.HttpConnect(ip, utils.RPC_PORT)
	defer client.Close()
	err := client.Call("Node.StartReconciliationRPC", args, &reply)
	if err != nil {
		utils.PrintTs("StartReconciliationRPC error: " + err.Error())
	}
}

/*
Esegue tutte le operazioni per rendere il registry Up & Running
*/
func InitRegistry() *http.Server {
	utils.PrintHeaderL1("REGISTRY SETUP")

	server := &http.Server{
		Addr:    utils.REGISTRY_PORT,
		Handler: http.DefaultServeMux,
	}
	service := InitializeDHTService()
	rpc.Register(service)
	rpc.HandleHTTP()

	go server.ListenAndServe()
	utils.PrintTs("Service Registry waiting for incoming connections")

	go StartCheckTerminatingNodes()
	go StartPeriodicReconciliation()
	return server
}
