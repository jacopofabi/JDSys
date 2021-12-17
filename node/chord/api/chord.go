/*
Insieme di strutture e metodi per implementare l'algoritmo Chord
*/
package api

import (
	"JDSys/utils"
	"crypto/sha256"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"
)

/*
Mantiene le informazioni riguardo un Nodo Chord
*/
type NodeInfo struct {
	id     [sha256.Size]byte
	ipaddr string
}

/*
Struttura dati utilizzata per l'aggiornamento del nodo
*/
type request struct {
	write bool
	succ  bool
	index int
}

/*
Identifica un server chord che partecipa all'algoritmo di lookup
*/
type ChordNode struct {
	predecessor   *NodeInfo
	successor     *NodeInfo
	successorList [sha256.Size * 8]NodeInfo
	fingerTable   [sha256.Size*8 + 1]NodeInfo

	finger  chan NodeInfo
	request chan request

	id     [sha256.Size]byte
	ipaddr string

	connections  map[string]net.TCPConn
	applications map[byte]ChordApp
}

type PeerError struct {
	Address string
	Err     error
}

func (e *PeerError) Error() string {
	return fmt.Sprintf("Failed to connect to peer: %s. Cause of failure: %s.", e.Address, e.Err)
}

/*
Funzione che controlla gli errori
*/
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
	}
}

/* Ritorna l'indirizzo del successore della chiave nella DHT Chord.
Il processo di lookup è iterativo. Iniziando da un nodo specificato (start)
questa funzione richiede la finger table del nodo più vicino che precede la chiave,
finchè il successore della stessa non viene identificato

Se l'indirizzo di 'start' non è raggiungibile si ha un PeerError
*/
func Lookup(key [sha256.Size]byte, start string) (addr string, err error) {
	addr = start

	msg := getfingersMsg()
	reply, err := Send(msg, start)
	if err != nil { //node failed.
		err = &PeerError{start, err}
		return
	}

	ft, err := parseFingers(reply)
	if err != nil {
		err = &PeerError{start, err}
		return
	}
	if len(ft) < 2 {
		return
	}

	current := ft[0]

	if key == current.id {
		addr = current.ipaddr
		return
	}

	// Ciclo sulla Finger Table
	for i := len(ft) - 1; i > 0; i-- {
		f := ft[i]
		if i == 0 {
			break
		}
		if InRange(f.id, current.id, key) { //see if f.id is closer than I am.
			addr, err = Lookup(key, f.ipaddr)
			if err != nil { //node failed
				continue
			}
			return
		}
	}
	addr = ft[1].ipaddr
	msg = pingMsg()
	_, err = Send(msg, addr)

	// Chiede al nodo la sua successor list
	if err != nil {
		msg = getsuccessorsMsg()
		reply, err = Send(msg, current.ipaddr)
		if err != nil {
			addr = current.ipaddr
			return
		}

		ft, err = parseFingers(reply)
		if err != nil {
			addr = current.ipaddr
			return
		}

		for i := 0; i < len(ft); i++ {
			f := ft[i]
			if i == 0 {
				break
			}
			msg = pingMsg()
			_, err = Send(msg, f.ipaddr)
			if err != nil { //closest next successor that responds
				addr = f.ipaddr
				return
			}
		}

		addr = current.ipaddr
		return
	}

	return
}

/*
Restituisce il successore del nodo
*/
func (node *ChordNode) GetSuccessor() *NodeInfo {
	return node.successor
}

/*
Restituisce il predecessore del nodo
*/
func (node *ChordNode) GetPredecessor() *NodeInfo {
	return node.predecessor
}

/*
Restituisce l'indirizzo IP del nodo
*/
func (node *ChordNode) GetIpAddress() string {
	return utils.RemovePort(node.ipaddr)
}

/*
Ritorna l'indirizzo IP del nodo responsabile della chiave key cercata
*/
func (node *ChordNode) lookup(key [sha256.Size]byte, start string) (addr string, err error) {

	addr = start

	msg := getfingersMsg()
	reply, err := node.send(msg, start)
	if err != nil { //node failed
		err = &PeerError{start, err}
		return
	}

	ft, err := parseFingers(reply)
	if err != nil {
		err = &PeerError{start, err}
		return
	}
	if len(ft) < 2 {
		return
	}

	current := ft[0]

	if key == current.id {
		addr = current.ipaddr
		return
	}

	//loop through finger table and see what the closest finger is
	for i := len(ft) - 1; i > 0; i-- {
		f := ft[i]
		if i == 0 {
			break
		}
		if InRange(f.id, current.id, key) { //see if f.id is closer than I am.
			addr, err = node.lookup(key, f.ipaddr)
			if err != nil { //node failed
				continue
			}
			return
		}
	}
	addr = ft[1].ipaddr
	msg = pingMsg()
	_, err = node.send(msg, addr)

	//this code is executed if the id's successor has gone missing
	if err != nil {
		//ask node for its successor list
		msg = getsuccessorsMsg()
		reply, err = node.send(msg, current.ipaddr)
		if err != nil {
			addr = current.ipaddr
			return
		}
		ft, err = parseFingers(reply)
		if err != nil {
			addr = current.ipaddr
			return
		}

		for i := 0; i < len(ft); i++ {
			f := ft[i]
			if i == 0 {
				break
			}
			msg = pingMsg()
			_, err = node.send(msg, f.ipaddr)
			if err != nil { //closest next successor that responds
				addr = f.ipaddr
				return
			}
		}

		addr = current.ipaddr
		return
	}

	return
}

/*
Crea un nuovo Chord DHT e ritorna il ChordNode originale
*/
func Create(myaddr string) *ChordNode {
	node := new(ChordNode)
	//initialize node information
	node.id = utils.HashString(myaddr)
	node.ipaddr = myaddr
	me := new(NodeInfo)
	me.id = node.id
	me.ipaddr = node.ipaddr
	node.fingerTable[0] = *me
	succ := new(NodeInfo)
	node.successor = succ
	pred := new(NodeInfo)
	node.predecessor = pred

	//set up channels for finger manager
	c := make(chan NodeInfo)
	c2 := make(chan request)
	node.finger = c
	node.request = c2

	//initialize listener and network manager threads
	node.listen(myaddr)
	node.connections = make(map[string]net.TCPConn)
	node.applications = make(map[byte]ChordApp)

	//initialize maintenance and finger manager threads
	go node.data()
	go node.maintain()
	return node
}

/*
Aggiunge un nuovo nodo ad un anello esistente. Inizia cercando il successore del nodo specificato in input.
Ritorna il nuovo ChordNode dopo che è stato inserito nella DHT.
Se l'indirizzo di partenza non è raggiungibile si ha un PeerError.
*/
func Join(myaddr string, addr string) (*ChordNode, error) {
	node := Create(myaddr)
	successor, err := Lookup(node.id, addr)
	if err != nil || successor == "" {
		return nil, &PeerError{addr, err}
	}

	// Trova l'ID del nodo
	msg := getidMsg()
	reply, err := Send(msg, successor)
	if err != nil {
		return nil, &PeerError{addr, err}
	}

	// Aggiorna il nodo includendo il suo successore
	succ := new(NodeInfo)
	succ.id, err = parseId(reply)
	if err != nil {
		return nil, &PeerError{addr, err}
	}
	succ.ipaddr = successor
	node.query(true, false, 1, succ)

	return node, nil
}

/*
Gestisce le operazioni di lettura e scrittura sulla struttura dati del nodo
*/
func (node *ChordNode) data() {
	var i int
	exist := false
	for {
		req := <-node.request
		if req.write {
			if req.succ {
				node.successorList[req.index] = <-node.finger
			} else {
				if req.index < 0 {
					*node.predecessor = <-node.finger
				} else if req.index == 1 {
					*node.successor = <-node.finger
					node.fingerTable[1] = *node.successor
					node.successorList[0] = *node.successor
				} else {
					prova := <-node.finger
					for i = 0; i < len(node.fingerTable); i++ {
						if prova == node.fingerTable[i] {
							exist = true
						} else {
							continue
						}
					}
					if !exist {
						node.fingerTable[req.index] = prova
					}
				}
			}
		} else { //req.read
			if req.succ {
				node.finger <- node.successorList[req.index]
			} else {
				if req.index < 0 {
					node.finger <- *node.predecessor
				} else {
					node.finger <- node.fingerTable[req.index]
				}
			}
		}
	}
}

/*
Permette ad una funzione di leggere o scrivere un oggetto del nodo
*/
func (node *ChordNode) query(write bool, succ bool, index int, newf *NodeInfo) NodeInfo {
	f := new(NodeInfo)
	req := request{write, succ, index}
	node.request <- req
	if write {
		node.finger <- *newf
	} else {
		*f = <-node.finger
	}

	return *f
}

/*
Esegue periodicamente operazioni di mantenimento
*/
func (node *ChordNode) maintain() {
	ctr := 0
	for {
		//stabilize
		node.stabilize()
		//check predecessor
		node.checkPred()
		//update fingers
		node.fix(ctr)
		ctr = ctr % 256
		ctr += 1
		time.Sleep(utils.CHORD_FIX_INTERVAL)
	}
}

/*
Garantisce che per ogni nodo il successore del predecessore sia il nodo stesso
*/
func (node *ChordNode) stabilize() {
	successor := node.query(false, false, 1, nil)

	if successor.zero() {
		return
	}

	msg := pingMsg()
	_, err := node.send(msg, successor.ipaddr)
	if err != nil {
		//successor failed to respond
		//check in successor list for next available successor.
		for i := 1; i < sha256.Size*8; i++ {
			successor = node.query(false, true, i, nil)
			if successor.ipaddr == node.ipaddr {
				continue
			}
			msg := pingMsg()
			_, err = node.send(msg, successor.ipaddr)
			if err == nil {
				break
			} else {
				successor.ipaddr = ""
			}
		}
		node.query(true, false, 1, &successor)
		if successor.ipaddr == "" {
			return
		}
	}

	//everything is OK, update successor list
	msg = getsuccessorsMsg()
	reply, err := node.send(msg, successor.ipaddr)
	if err != nil {
		return
	}
	ft, err := parseFingers(reply)
	if err != nil {
		return
	}
	for i := range ft {
		if i < sha256.Size*8-1 {
			node.query(true, true, i+1, &ft[i])
		}
	}

	//ask sucessor for predecessor
	msg = getpredMsg()
	reply, err = node.send(msg, successor.ipaddr)
	if err != nil {
		return
	}

	predOfSucc, err := parseFinger(reply)
	if err != nil { //node failed
		return
	}
	if predOfSucc.ipaddr != "" {
		if predOfSucc.id != node.id {
			if InRange(predOfSucc.id, node.id, successor.id) {
				node.query(true, false, 1, &predOfSucc)
			}
		} else { //everything is fine
			return
		}
	}

	//claim to be predecessor of succ
	me := new(NodeInfo)
	me.id = node.id
	me.ipaddr = node.ipaddr
	msg = claimpredMsg(*me)
	node.send(msg, successor.ipaddr)

}

/*
Permette ad una applicazione di un nodo chord di registrarsi per ricevere notifiche e messaggi
tramite il DHT Chord.

Un nodo chord registra un'app e propaga tutti i messaggi chiamanda il metodo dell'interfaccia Message.
Le applicazioni saranno anche notificate di ogni cambiamento fatto nel predecessore del nodo
*/

func (node *ChordNode) Register(id byte, app ChordApp) bool {
	if _, ok := node.applications[id]; ok {
		return false
	}
	node.applications[id] = app
	return true

}

func (node *ChordNode) notify(newPred NodeInfo) {
	node.query(true, false, -1, &newPred)
	//update predecessor
	successor := node.query(false, false, 1, nil)
	if successor.zero() {
		node.query(true, false, 1, &newPred)
	}
	//notify applications
	for _, app := range node.applications {
		app.Notify(newPred.id, node.id, newPred.ipaddr)
	}
}

func (node *ChordNode) checkPred() {
	predecessor := node.query(false, false, -1, nil)
	if predecessor.zero() {
		return
	}

	msg := pingMsg()
	reply, err := node.send(msg, predecessor.ipaddr)
	if err != nil {
		predecessor.ipaddr = ""
		node.query(true, false, -1, &predecessor)
	}

	if success, err := parsePong(reply); !success || err != nil {
		predecessor.ipaddr = ""
		node.query(true, false, -1, &predecessor)
	}
}

func (node *ChordNode) fix(which int) {
	successor := node.query(false, false, 1, nil)
	if which == 0 || which == 1 || successor.zero() {
		return
	}
	var targetId [sha256.Size]byte
	copy(targetId[:sha256.Size], target(node.id, which)[:sha256.Size])
	newip, err := node.lookup(targetId, successor.ipaddr)
	if err != nil {
		checkError(err)
		return
	}
	if newip == node.ipaddr {
		checkError(err)
		return
	}

	//find id of node
	msg := getidMsg()
	reply, err := node.send(msg, newip)
	if err != nil {
		checkError(err)
		return
	}

	newfinger := new(NodeInfo)
	newfinger.ipaddr = newip
	newfinger.id, _ = parseId(reply)
	node.query(true, false, which, newfinger)

}

/*
Funzione ausiliaria che ritorna true se il valore x è compreso tra (min,max)
*/
func InRange(x [sha256.Size]byte, min [sha256.Size]byte, max [sha256.Size]byte) bool {
	xint := new(big.Int)
	maxint := new(big.Int)
	minint := new(big.Int)
	xint.SetBytes(x[:sha256.Size])
	minint.SetBytes(min[:sha256.Size])
	maxint.SetBytes(max[:sha256.Size])

	if xint.Cmp(minint) == 1 && maxint.Cmp(xint) == 1 {
		return true
	}

	if maxint.Cmp(xint) == 1 && minint.Cmp(maxint) == 1 {
		return true
	}

	if minint.Cmp(maxint) == 1 && xint.Cmp(minint) == 1 {
		return true
	}

	return false
}

/*
Ritorna il target ID usato dalla funzione fix
*/
func target(me [sha256.Size]byte, which int) []byte {
	meint := new(big.Int)
	meint.SetBytes(me[:sha256.Size])

	baseint := new(big.Int)
	baseint.SetUint64(2)

	powint := new(big.Int)
	powint.SetInt64(int64(which - 1))

	var biggest [sha256.Size + 1]byte
	for i := range biggest {
		biggest[i] = 255
	}

	tmp := new(big.Int)
	tmp.SetInt64(1)

	modint := new(big.Int)
	modint.SetBytes(biggest[:sha256.Size])
	modint.Add(modint, tmp)

	target := new(big.Int)
	target.Exp(baseint, powint, modint)
	target.Add(meint, target)
	target.Mod(target, modint)

	bytes := target.Bytes()
	diff := sha256.Size - len(bytes)
	if diff > 0 {
		tmp := make([]byte, sha256.Size)
		//pad with zeros
		for i := 0; i < diff; i++ {
			tmp[i] = 0
		}
		for i := diff; i < sha256.Size; i++ {
			tmp[i] = bytes[i-diff]
		}
		bytes = tmp
	}
	return bytes[:sha256.Size]
}

func (f NodeInfo) String() string {
	return f.ipaddr
}

func (f NodeInfo) zero() bool {
	if f.ipaddr == "" {
		return true
	} else {
		return false
	}
}

/** Printouts of information **/

/*
Ritorna una stringa contenente l'indirizzo ip del nodo, il successore ed il predecessore
*/
func (node *ChordNode) String() string {
	var succ, pred string
	successor := node.query(false, false, 1, nil)
	predecessor := node.query(false, false, -1, nil)
	if !successor.zero() {
		succ = successor.String()
	} else {
		succ = "Unknown"
	}
	if !predecessor.zero() {
		pred = predecessor.String()
	} else {
		pred = "Unknown"
	}
	return fmt.Sprintf("%s\t%s\t%s\n", node.ipaddr, succ, pred)
}

/*
Ritorna una stringa che rappresenta la finger table del ChordNode
*/
func (node *ChordNode) ShowFingers() string {
	retval := ""
	finger := new(NodeInfo)
	prevfinger := new(NodeInfo)
	ctr := 0
	for i := 0; i < sha256.Size*8+1; i++ {
		*finger = node.query(false, false, i, nil)
		if !finger.zero() {
			ctr += 1
			if i == 0 || finger.ipaddr != prevfinger.ipaddr {
				retval += fmt.Sprintf("%d %s\n", i, finger.String())
			}
		}
		*prevfinger = *finger
	}
	return retval + fmt.Sprintf("Total fingers: %d.\n", ctr)
}

/*
Ritorna una stringa che rappresenta la lista dei successori del ChordNode
*/
func (node *ChordNode) ShowSucc() string {
	table := ""
	finger := new(NodeInfo)
	prevfinger := new(NodeInfo)
	for i := 0; i < sha256.Size*8; i++ {
		*finger = node.query(false, true, i, nil)
		if finger.ipaddr != "" {
			if i == 0 || finger.ipaddr != prevfinger.ipaddr {
				table += fmt.Sprintf("%s\n", finger.String())
			}
		}
		*prevfinger = *finger
	}
	return table
}

/** Chord application interface and methods **/

/*
Interfaccia per le applicazioni che devono essere eseguite sopra un Chord DHT
*/
type ChordApp interface {

	//Notify will alert the application of changes in the ChordNode's predecessor
	Notify(id [sha256.Size]byte, me [sha256.Size]byte, addr string)

	//Message will forward a message that was received through the DHT to the application
	Message(data []byte) []byte
}

/*
Ritorna l'indirizzo IP del nodo senza il suo numero di porta chord
*/
func (info *NodeInfo) GetIpAddr() string {
	if info.ipaddr == "" {
		return ""
	} else {
		return utils.RemovePort(info.ipaddr)
	}
}
