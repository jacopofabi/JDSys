package utils

import (
	"fmt"
	"strings"
	"time"
)

var HL int = 80

/*
Ritorna il valore attuale del tempo
*/
func GetTimestamp() time.Time {
	return time.Now()
}

/*
Ritorna una stringa con il valore del tempo formattato
*/
func FormatTime(t time.Time) string {
	return t.Format("15:04:05.0000")
}

/*
Stampa a schermo un timestamp
*/
func PrintFormattedTimestamp() {
	fmt.Print("[" + FormatTime(GetTimestamp()) + "] ")
}

/*
Stampa a schermo un messaggio, includendo un timestamp formattato
*/
func PrintTs(message string) {
	ts := "[" + FormatTime(GetTimestamp()) + "] "
	fmt.Print(ts + message + "\n")
}

/*
Stampa un messaggio formattandolo come Header di Livello 1
*/
func PrintHeaderL1(message string) {
	center := (HL-len(message))/2 - 2
	before := strings.Repeat("═", center) + "╣ "
	after := " ╠" + strings.Repeat("═", center) + "\n"
	fmt.Print(before + message + after)
}

/*
Stampa un messaggio formattandolo come Header di Livello 2
*/
func PrintHeaderL2(message string) {
	fmt.Println("\n" + strings.Repeat("—", HL))
	PrintTs(message)
	fmt.Println(strings.Repeat("—", HL))
}

/*
Stampa un messaggio formattandolo come Header di Livello 3
*/
func PrintHeaderL3(message string) {
	fmt.Println("\n" + strings.Repeat("-", HL))
	PrintTs(message)
	fmt.Println(strings.Repeat("-", HL))
}

/*
Stampa una linea di Livello 1
*/
func PrintLineL1() {
	fmt.Println(strings.Repeat("═", HL))
}

/*
Stampa una linea di Livello 2
*/
func PrintLineL2() {
	fmt.Println(strings.Repeat("—", HL))
}

/*
Ritorna una stringa formattata all'interno di un Box
*/
func StringInBox(message string) string {
	top := "+" + strings.Repeat("—", len(message)+2) + "+\n"
	middle := "| " + message + " |\n"

	return top + middle
}

/*
Stampa una stringa formattata all'interno di un Box
*/
func PrintInBox(message string) {
	line := "+" + strings.Repeat("—", len(message)+2) + "+\n"
	middle := "| " + message + " |\n"

	fmt.Print(line + middle + line)
}

/*
Formatta due messaggi all'interno di un unico Box
*/
func StringInBoxL2(msg1 string, msg2 string) string {
	var lenght int
	var diff1 int
	var diff2 int
	if len(msg1) >= len(msg2) {
		lenght = len(msg1)
		diff1 = 0
		diff2 = lenght - len(msg2)
	} else {
		lenght = len(msg2)
		diff2 = 0
		diff1 = lenght - len(msg1)
	}
	top := "+" + strings.Repeat("—", lenght+2) + "+\n"
	middle1 := "| " + msg1 + strings.Repeat(" ", diff1) + " |\n"
	middle2 := "| " + msg2 + strings.Repeat(" ", diff2) + " |\n"
	bottom := "+" + strings.Repeat("—", lenght+2) + "+"

	return top + middle1 + middle2 + bottom
}

/*
Stampa due messaggi, formattandoli all'interno di un unico Box
*/
func PrintStringInBoxL2(msg1 string, msg2 string) {
	var lenght int
	var diff1 int
	var diff2 int
	if len(msg1) >= len(msg2) {
		lenght = len(msg1)
		diff1 = 0
		diff2 = lenght - len(msg2)
	} else {
		lenght = len(msg2)
		diff2 = 0
		diff1 = lenght - len(msg1)
	}
	top := "+" + strings.Repeat("—", lenght+2) + "+\n"
	middle1 := "| " + msg1 + strings.Repeat(" ", diff1) + " |\n"
	middle2 := "| " + msg2 + strings.Repeat(" ", diff2) + " |\n"
	bottom := "+" + strings.Repeat("—", lenght+2) + "+"

	fmt.Println(top + middle1 + middle2 + bottom)
}

/*
Stampa la lista di comandi disponibili sul client
*/
func PrintClientCommandsList() {
	fmt.Print(StringInBox("COMMANDS LIST"))

	get := "Get"
	put := "Put"
	del := "Delete"
	app := "Append"
	ext := "Exit"

	top := "+" + strings.Repeat("—", 15) + "+\n"
	row1 := "| 1 |  " + get + strings.Repeat(" ", 3) + "   |\n"
	row2 := "| 2 |  " + put + strings.Repeat(" ", 3) + "   |\n"
	row3 := "| 3 |  " + del + strings.Repeat(" ", 0) + "   |\n"
	row4 := "| 4 |  " + app + strings.Repeat(" ", 0) + "   |\n"
	row5 := "| 5 |  " + ext + strings.Repeat(" ", 2) + "   |\n"
	bottom := "+" + strings.Repeat("—", 15) + "+"

	fmt.Println(top + row1 + row2 + row3 + row4 + row5 + bottom)
	PrintLineL1()
}

/*
Stampa la Titlebar sul client
*/
func PrintClientTitlebar() {
	PrintHeaderL1("JDSys Key-Value Storage")
}
