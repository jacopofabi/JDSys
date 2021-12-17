package impl

import (
	"JDSys/utils"
	"fmt"
	"strconv"
	"time"
)

/*
Effettua una richiesta di Put, una di Update, una di Get, una di Append e una di Delete, ritornando il tempo medio di risposta
*/
func MeasureResponseTimeIteration(iteration int) time.Duration {

	utils.PrintHeaderL3("Iteration #" + strconv.Itoa(iteration))
	rt1 := MeasurePut("rt_key", "rt_value")
	rt2 := MeasurePut("rt_key", "rt_value_upd")
	rt3 := MeasureGet("rt_key")
	rt4 := MeasureAppend("rt_key", "rt_value_app")
	rt5 := MeasureDelete("rt_key")

	total := rt1 + rt2 + rt3 + rt4 + rt5
	meanRt := total / 5
	return meanRt
}

/*
Esegue 10 iterazioni di MeasureResponseTimeIteration calcolando poi la media dei tempi di risposta ottenuti in ogni iterazione.
*/
func MeasureResponseTime() {
	utils.ClearScreen()
	utils.PrintHeaderL1("TESTING SUBSYSTEM")
	utils.PrintLineL1()
	fmt.Println("Press enter to start measuring response time")
	EnterToContinue()

	utils.ClearScreen()
	utils.PrintHeaderL1("TESTING SUBSYSTEM")

	var total time.Duration = 0
	for i := 0; i < 10; i++ {
		total += MeasureResponseTimeIteration(i)
	}
	meanRt := total / 10
	utils.PrintLineL2()
	fmt.Println("Mean Response Time:", meanRt)
	utils.PrintLineL2()
	EnterToContinue()
}

/*
Misura il tempo di risposta di una operazione GET
*/
func MeasureGet(key string) time.Duration {
	utils.PrintTs("Measuring Get...")

	start := utils.GetTimestamp()
	GetRPC(key)
	end := utils.GetTimestamp()
	ts := end.Sub(start)
	utils.PrintTs(fmt.Sprintf("Get Executed in %f", ts.Seconds()))
	return ts
}

/*
Misura il tempo di risposta di una operazione PUT
*/
func MeasurePut(key string, value string) time.Duration {
	utils.PrintTs("Measuring Put...")

	start := utils.GetTimestamp()
	PutRPC(key, value)
	end := utils.GetTimestamp()
	ts := end.Sub(start)
	utils.PrintTs(fmt.Sprintf("Put Executed in %f", ts.Seconds()))
	return ts
}

/*
Misura il tempo di risposta di una operazione APPEND
*/
func MeasureAppend(key string, value string) time.Duration {
	utils.PrintTs("Measuring Append...")

	start := utils.GetTimestamp()
	AppendRPC(key, value)
	end := utils.GetTimestamp()

	ts := end.Sub(start)
	utils.PrintTs(fmt.Sprintf("Append Executed in %f", ts.Seconds()))
	return ts
}

/*
Misura il tempo di risposta di una operazione DELETE
*/
func MeasureDelete(key string) time.Duration {
	utils.PrintTs("Measuring Delete...")
	start := utils.GetTimestamp()
	DeleteRPC(key)
	end := utils.GetTimestamp()
	ts := end.Sub(start)
	utils.PrintTs(fmt.Sprintf("Delete Executed in %f", ts.Seconds()))
	return ts
}
