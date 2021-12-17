package mongo

import (
	"JDSys/utils"
	"fmt"
	"time"
)

/*
Identifica un'entry di tipo {chiave,valore}, includendo il timestamp relativo
alla sua ultima modifica, ed il timestamp relativo al suo ultimo accesso in lettura/scrittura
*/
type MongoEntry struct {
	Key      string
	Value    string
	Timest   time.Time
	LastAcc  time.Time
	Conflict bool // rende piu efficiente il merge delle entry
}

/*
Formatta l'entry includendo il relativo timestamp
*/
func (me *MongoEntry) Format() string {
	return fmt.Sprintf("{ %s , %s , %s }", me.Key, me.Value, me.Timest.String())
}

/*
Formatta l'entry per essere visualizzata dal client.
*/
func (me *MongoEntry) FormatClient() string {
	key := "Key   | " + me.Key
	value := "Value | " + utils.RemoveBrackets(me.Value)
	return utils.StringInBoxL2(key, value)
}
