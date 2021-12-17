package communication

import (
	"JDSys/utils"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Dimensione del buffer per trasferire il file di aggiornamento
const BUFFERSIZE = 1024

/*
Goroutine in cui ogni nodo è in attesa di connessioni per ricevere l'export CSV del DB di altri nodi. Tramite mode si specifica
il servizio specifico, e quindi la porta su cui il nodo si metterà in ascolto
*/
func StartReceiver(fileChannel chan string, mutex *sync.Mutex, mode string) {
	var port string
	switch mode {
	case utils.REPLN:
		port = utils.FILETR_REPLICATION_PORT
	case utils.RECON:
		port = utils.FILETR_RECONCILIATION_PORT
	case utils.MIGRN:
		port = utils.FILETR_MIGRATION_PORT
	}
	server, err := net.Listen("tcp", port)
	if err != nil {
		utils.PrintTs("Listening Error: " + err.Error())
	}
	for {
		connection, err := server.Accept()
		if err != nil {
			utils.PrintTs("Accept Error: " + err.Error())
		}
		receiveFile(connection, fileChannel, mutex, mode)
		connection.Close()
	}
}

/*
Apre la connessione verso un altro nodo per trasmettere un file. Mode specifica il servizio su cui si vuole inviare il messaggio, e quindi
su quale porta inviare il file CSV
*/
func StartSender(filename string, address string, mode string) error {
	var addr string
	switch mode {
	case utils.REPLN:
		addr = address + utils.FILETR_REPLICATION_PORT
	case utils.RECON:
		addr = address + utils.FILETR_RECONCILIATION_PORT
	case utils.MIGRN:
		addr = address + utils.FILETR_MIGRATION_PORT
	}
	connection, err := net.DialTimeout("tcp", addr, 20*time.Second)
	if err != nil {
		utils.PrintTs(err.Error())
		return err
	}
	defer connection.Close()
	utils.PrintTs("Ready to send DB export")
	return sendFile(connection, filename)
}

/*
Utility per ricevere un file tramite la connessione
*/
func receiveFile(connection net.Conn, fileChannel chan string, mutex *sync.Mutex, mode string) {
	var receivedBytes int64
	var newFile *os.File
	var err error

	bufferFileSize := make([]byte, 10)

	mutex.Lock()
	connection.Read(bufferFileSize)
	fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)

	switch mode {
	case utils.REPLN:
		utils.PrintHeaderL2("A node wants to send his replica updates via TCP")
		newFile, err = os.Create(utils.REPLICATION_RECEIVE_FILE)

	case utils.RECON:
		utils.PrintHeaderL2("A node wants to send a Reconciliation message via TCP")
		newFile, err = os.Create(utils.RECONCILIATION_RECEIVE_FILE)

	case utils.MIGRN:
		utils.PrintHeaderL2("A node wants to send his entries via TCP")
		newFile, err = os.Create(utils.MIGRATION_RECEIVE_FILE)
	}

	if err != nil {
		utils.PrintTs(err.Error())
	}
	for {
		if (fileSize - receivedBytes) < BUFFERSIZE {
			io.CopyN(newFile, connection, (fileSize - receivedBytes))
			connection.Read(make([]byte, (receivedBytes+BUFFERSIZE)-fileSize))
			break
		}
		io.CopyN(newFile, connection, BUFFERSIZE)
		receivedBytes += BUFFERSIZE
	}
	defer newFile.Close()
	utils.PrintTs("File received correctly")
	fileChannel <- "rcvd"
}

/*
Utility per inviare un file tramite la connessione
*/
func sendFile(connection net.Conn, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		utils.PrintTs(err.Error())
		return err
	}
	fileInfo, err := file.Stat()
	if err != nil {
		utils.PrintTs(err.Error())
		return err
	}

	fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
	connection.Write([]byte(fileSize))
	sendBuffer := make([]byte, BUFFERSIZE)
	utils.PrintTs("Start sending file via TCP")
	for {
		_, err = file.Read(sendBuffer)
		if err == io.EOF {
			break
		}
		connection.Write(sendBuffer)
	}
	utils.PrintTs("File sent correctly!")
	return nil
}

/*
Riempie una stringa per raggiungere una lunghezza specificata
*/
func fillString(retunString string, toLength int) string {
	for {
		lengtString := len(retunString)
		if lengtString < toLength {
			retunString = retunString + ":"
			continue
		}
		break
	}
	return retunString
}
