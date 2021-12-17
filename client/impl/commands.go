package impl

import (
	"JDSys/utils"
	"bufio"
	"fmt"
	"os"
	"strings"
)

/*
Permette al client di recuperare il valore associato ad una precisa chiave contattando il LB
*/
func Get() {
	utils.ClearScreen()
	utils.PrintClientTitlebar()
	utils.PrintInBox("GET")
	utils.PrintLineL1()
	SecScanln("> Insert the Key of the desired entry")
	utils.PrintLineL1()
	//GetRPC(key)
	key := "Key   | " + "1"
	value := "Value | " + "Prova"
	fmt.Println(utils.StringInBoxL2(key, value))
	EnterToContinue()
}

/*
Permette al client di inserire una coppia key-value nel sistema di storage contattando il LB
*/
func Put() {
	utils.ClearScreen()
	utils.PrintClientTitlebar()
	utils.PrintInBox("PUT")
	utils.PrintLineL1()
	key := SecScanln("> Insert the Entry Key")
	value := SecScanln("> Insert the Entry Value")
	utils.PrintLineL1()
	PutRPC(key, value)
	EnterToContinue()
}

/*
Permette al client di aggiungere un nuovo valore ad una chiave presente nel sistema di storage contattando il LB
*/
func Append() {
	utils.ClearScreen()
	utils.PrintClientTitlebar()
	utils.PrintInBox("APPEND")
	utils.PrintLineL1()
	key := SecScanln("> Insert the Key of the Entry to Update")
	newValue := SecScanln("> Insert the Value to Append")
	utils.PrintLineL1()
	AppendRPC(key, newValue)
	EnterToContinue()
}

/*
Permette al client di eliminare una coppia key-value dal sistema di storage contattando il LB
*/
func Delete() {
	utils.ClearScreen()
	utils.PrintClientTitlebar()
	utils.PrintInBox("DELETE")
	utils.PrintLineL1()
	key := SecScanln("> Insert the Key of the Entry to Delete")
	utils.PrintLineL1()
	DeleteRPC(key)
	EnterToContinue()
}

/*
Termina il programma client.
*/
func Exit() {
	utils.PrintLineL1()
	fmt.Println("> Closing Client...")
	fmt.Println("> Goodbye.")
	utils.PrintLineL1()
	fmt.Println("")
	os.Exit(0)
}

/*
Prende input da tastiera in modo sicuro, rimuovendo eventuali caratteri che potrebbero
permettere ad un attaccante di rompere la sintassi MongoDB
*/
func SecScanln(message string) string {
	arg := ""
	for {
		fmt.Print(message + ": ")
		arg, _ = bufio.NewReader(os.Stdin).ReadString('\n')
		if strings.ContainsAny(arg, "[]{},:./*()\\#") {
			fmt.Println("Inserted value contains not allowed characters []{},:./*()\\#")
			fmt.Println("Retry")
		} else if arg == "\n" {
		} else {
			break
		}
	}
	return arg[:len(arg)-1]
}

/*
Mette in pausa il programma fino alla pressione del tasto 'Enter'
*/
func EnterToContinue() {
	utils.PrintLineL2()
	fmt.Println("")
	fmt.Println("Press the Enter Key to continue...")
	fmt.Scanln()
}
