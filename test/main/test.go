package main

import (
	"JDSys/test/impl"
	"JDSys/utils"
	"fmt"
	"os"
	"strconv"
	"time"
)

var PERC_75 float32 = 0.75
var PERC_15 float32 = 0.15
var PERC_40 float32 = 0.40
var PERC_20 float32 = 0.20

var WORKLOAD []int

func main() {
	utils.ClearScreen()
	utils.PrintHeaderL1("TEST CLIENT")
	if len(os.Args) != 3 {
		fmt.Println("You need to specify the workload type to test.")
		fmt.Println("Usage: go run test.go WORKLOAD SIZE")
		return
	}
	test_type := os.Args[1]
	test_size_int, _ := strconv.Atoi(os.Args[2])
	test_size := float32(test_size_int)

	fmt.Println("Test PID:", os.Getpid())

	switch test_type {
	case "workload1":
		workload1(test_size)
	case "workload2":
		workload2(test_size)
	}
	select {}
}

/*
Esegue un test in cui il workload è composto:
- 85% operazioni di Get
- 15% operazioni di Put
E' possibile specificare tramite il parametro size il numero totali di query da eseguire.
*/
func workload1(size float32) {
	numGet := int(PERC_75 * size)
	numPut := int(PERC_15 * size)

	impl.WORKLOAD_GET = make([]int, numGet)
	impl.WORKLOAD_PUT = make([]int, numPut)

	utils.PrintHeaderL2("Start Spawning Threads for Workload 1")
	utils.PrintStringInBoxL2("# Get | "+strconv.Itoa(numGet), "# Put | "+strconv.Itoa(numPut))
	utils.PrintLineL2()
	time.Sleep(3 * time.Second)

	go runPutQueries(numPut)
	go runGetQueries(numGet)
}

/*
Esegue un test in cui il workload è composto:
- 40% operazioni di Get
- 40% operazioni di Put
- 20% operazioni di Append
E' possibile specificare tramite il parametro size il numero totali di query da eseguire.
*/
func workload2(size float32) {
	numGet := int(PERC_40 * size)
	numPut := int(PERC_40 * size)
	numApp := int(PERC_20 * size)

	impl.WORKLOAD_GET = make([]int, numGet)
	impl.WORKLOAD_PUT = make([]int, numPut)
	impl.WORKLOAD_APP = make([]int, numApp)

	utils.PrintHeaderL2("Start Spawning Threads for Workload 2")
	utils.PrintStringInBoxL2("# Get | "+strconv.Itoa(numGet), "# Put | "+strconv.Itoa(numPut))
	utils.PrintLineL2()
	time.Sleep(3 * time.Second)

	go runGetQueries(numGet)
	go runPutQueries(numPut)
	go runAppendQueries(numApp)
}

/*
Mantiene in esecuzione num Thread di Get.
*/
func runGetQueries(num int) {
	id := 0
	round := 0
	for {
		if id == num {
			id = 0
			round++
			utils.PrintTs("Get New Round Started: " + strconv.Itoa(round))
		}
		key := "test_key_" + strconv.Itoa(round) + "_" + strconv.Itoa(id)
		if impl.WORKLOAD_GET[id] != 1 {
			utils.PrintTs(fmt.Sprintf("Get Thread { %d , %d } spawned", round, id))
			go impl.TestGet(key, false, id)
		}
		id++
	}
}

/*
Mantiene in esecuzione num Thread di Put.
*/
func runPutQueries(num int) {
	id := 0
	round := 0
	for {
		if id == num {
			id = 0
			round++
			utils.PrintTs("Put New Round Started: " + strconv.Itoa(round))
		}
		key := "test_key_" + strconv.Itoa(round) + "_" + strconv.Itoa(id)
		value := "_test_val_" + strconv.Itoa(round) + "_" + strconv.Itoa(id)
		if impl.WORKLOAD_PUT[id] != 1 {
			utils.PrintTs(fmt.Sprintf("Put Thread { %d , %d } spawned", round, id))
			go impl.TestPut(key, value, false, id)
		}
		id++
	}
}

/*
Mantiene in esecuzione num Thread di Append.
*/
func runAppendQueries(num int) {
	id := 0
	round := 0
	for {
		if id == num {
			id = 0
			round++
			utils.PrintTs("Put New Round Started: " + strconv.Itoa(round))
		}
		key := "test_key_" + strconv.Itoa(round) + "_" + strconv.Itoa(id)
		arg := "_test_arg_" + strconv.Itoa(round) + "_" + strconv.Itoa(id)

		if impl.WORKLOAD_GET[id] != 1 {
			utils.PrintTs(fmt.Sprintf("Append Thread { %d , %d } spawned", round, id))
			go impl.TestAppend(key, arg, false, id)
		}
		id++
	}
}
