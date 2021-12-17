package utils

import (
	"log"
	"net"
	"net/rpc"
	"time"
)

/*
Permette di instaurare una connessione HTTP con il server all'indirizzo e porta specificati.
*/
func HttpConnect(addr string, port string) (*rpc.Client, error) {
retry:
	client, err := rpc.DialHTTP("tcp", addr+port)
	if err != nil {
		time.Sleep(DIAL_RETRY)
		goto retry
	}
	return client, err
}

/*
Restituisce l'indirizzo IP in uscita preferito della macchina che hosta il nodo
*/
func GetOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}
