package main

import (
	"JDSys/client/impl"
	"JDSys/utils"
	"fmt"
	"time"
)

func main() {
	for {
		utils.ClearScreen()
		utils.PrintClientTitlebar()
		utils.PrintClientCommandsList()
		cmd := impl.SecScanln("Insert a Command")
		switch {
		case cmd == "1":
			impl.Get()
		case cmd == "2":
			impl.Put()
		case cmd == "3":
			impl.Delete()
		case cmd == "4":
			impl.Append()
		case cmd == "5":
			impl.Exit()
		case cmd == "T" || cmd == "t":
			impl.MeasureResponseTime()
		default:
			fmt.Println("Command not recognized. Retry.")
			time.Sleep(1 * time.Second)
		}
	}
}
