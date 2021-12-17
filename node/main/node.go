package main

import (
	nodesys "JDSys/node/impl"
	"JDSys/utils"
	"fmt"
	"io"
)

func main() {
	utils.ClearScreen()
	node := new(nodesys.Node)
	nodesys.InitNode(node)
	utils.PrintHeaderL1("NODE  SYSTEM")
	utils.PrintInBox("Debug Commands")
	fmt.Println("print\nfingers\nsucc\nclear")
	utils.PrintLineL1()

	// Ciclo in cui è possibile stampare lo stato attuale del nodo.
Loop:
	for {
		var cmd string
		_, err := fmt.Scan(&cmd)
		switch {
		// Stampa successore e predecessore
		case cmd == "print":
			utils.PrintTs(node.ChordClient.String())
		// Stampa la finger table
		case cmd == "fingers":
			utils.PrintTs(node.ChordClient.ShowFingers())
		// Stampa la lista di successori
		case cmd == "succ":
			utils.PrintTs(node.ChordClient.ShowSucc())
		case cmd == "clear":
			utils.ClearScreen()
		// Errore
		case err == io.EOF:
			break Loop
		}

	}
	select {}
}
