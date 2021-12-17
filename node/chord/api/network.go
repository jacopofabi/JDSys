package api

import (
	"JDSys/utils"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

//Send is a helper function for sending a message to a peer in the Chord DHT.
//It opens a connection to the Chord node with the IP address addr,
//sends the message msg, and waits for a reply
func Send(msg []byte, addr string) (reply []byte, err error) {

	if addr == "" {
		err = &PeerError{addr, nil}
		return nil, err
	}

	raddr := new(net.TCPAddr)
	raddr.IP = net.ParseIP(strings.Split(addr, ":")[0])
	raddr.Port, err = strconv.Atoi(strings.Split(addr, ":")[1])
	if err != nil {
		return
	}
	//open a TCP connection with raddr, laddr is chosen automatically
	//so we don't need to set a specific port and check if it's used
	newconn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		return
	}
	conn := *newconn
	checkError(err)
	if err != nil {
		return
	}
	defer conn.Close()
	_, err = conn.Write(msg)
	if err != nil {
		return
	}

	reply = make([]byte, 100000)
	n, err := conn.Read(reply)
	if err != nil {
		return
	}
	reply = reply[:n]

	return

}

//send for a node checks existing open connections
func (node *ChordNode) send(msg []byte, addr string) (reply []byte, err error) {
	if addr == "" {
		err = &PeerError{addr, nil}
		return nil, err
	}

	conn, ok := node.connections[addr]
	if !ok {
		laddr := new(net.TCPAddr)
		laddr.IP = net.ParseIP(strings.Split(node.ipaddr, ":")[0])
		laddr.Port = 0
		if err != nil {
			return
		}
		raddr := new(net.TCPAddr)
		raddr.IP = net.ParseIP(strings.Split(addr, ":")[0])
		raddr.Port, err = strconv.Atoi(strings.Split(addr, ":")[1])
		if err != nil {
			return
		}
		newconn, nerr := net.DialTCP("tcp", laddr, raddr)
		if nerr != nil {
			err = nerr
			return
		}
		err = newconn.SetDeadline(time.Now().Add(3 * time.Minute))
		checkError(err)
		conn = *newconn
		node.connections[addr] = conn
	}

	_, err = conn.Write(msg)
	conn.SetDeadline(time.Now().Add(3 * time.Minute))
	if err != nil {
		laddr := new(net.TCPAddr)
		laddr.IP = net.ParseIP(strings.Split(node.ipaddr, ":")[0])
		laddr.Port = 0

		raddr := new(net.TCPAddr)
		raddr.IP = net.ParseIP(strings.Split(addr, ":")[0])
		raddr.Port, err = strconv.Atoi(strings.Split(addr, ":")[1])
		if err != nil {
			return
		}
		newconn, nerr := net.DialTCP("tcp", laddr, raddr)
		if nerr != nil {
			err = nerr
			return
		}
		err = newconn.SetDeadline(time.Now().Add(3 * time.Minute))
		checkError(err)
		conn = *newconn
		_, err = conn.Write(msg)
		if err != nil {
			return
		}
		node.connections[addr] = conn
	}

	reply = make([]byte, 100000)
	n, err := conn.Read(reply)
	conn.SetDeadline(time.Now().Add(3 * time.Minute))
	if err != nil {
		return
	}
	reply = reply[:n]

	return

}

//Listens at an address for incoming messages
func (node *ChordNode) listen(addr string) {
	utils.PrintTs(fmt.Sprintf("Chord node %x is listening on %s", node.id, addr))
	c := make(chan []byte)
	c2 := make(chan []byte)
	go func() {
		defer utils.PrintTs("No longer listening")
		for {
			message := <-c
			node.parseMessage(message, c2)
		}
	}()

	//listen to TCP port
	laddr := new(net.TCPAddr)
	laddr.IP = net.ParseIP(strings.Split(addr, ":")[0])
	laddr.Port, _ = strconv.Atoi(strings.Split(addr, ":")[1])
	listener, err := net.ListenTCP("tcp", laddr)
	checkError(err)
	go func() {
		defer utils.PrintTs("No longer listening")
		for {
			if conn, err := listener.AcceptTCP(); err == nil {
				err = conn.SetDeadline(time.Now().Add(3 * time.Minute))
				checkError(err)
				go handleMessage(conn, c, c2)
			} else {
				checkError(err)
				continue
			}
		}
	}()
}

func handleMessage(conn net.Conn, c chan []byte, c2 chan []byte) {

	//Close conenction when function exits
	defer conn.Close()
	for {

		//Create data buffer of type byte slice
		data := make([]byte, 100000)
		conn.SetDeadline(time.Now().Add(3 * time.Minute))
		n, err := conn.Read(data)
		if n >= 4095 {
			utils.PrintTs("Ran out of buffer room.\n")
		}
		if err == io.EOF { //exit cleanly
			return
		}
		if err != nil {
			return
		}

		c <- data[:n]

		//wait for message to come back
		response := <-c2

		conn.SetDeadline(time.Now().Add(3 * time.Minute))
		n, err = conn.Write(response)
		if err != nil {
			utils.PrintTs("Uh oh (3)")
			checkError(err)
			return
		}
		if n > 100000 {
			utils.PrintTs(fmt.Sprintf("Uh oh. Wrote %d bytes.\n", n))
		}
	}
}
