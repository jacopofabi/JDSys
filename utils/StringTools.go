package utils

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

/*
Restituisce la sottostringa tramite stringa di inizio e fine specificate
*/
func GetStringInBetween(str string, startS string, endS string) (result string) {
	s := strings.Index(str, startS)
	if s == -1 {
		return result
	}
	newS := str[s+len(startS):]
	e := strings.Index(newS, endS)
	if e == -1 {
		return result
	}
	result = newS[:e]
	return result
}

/*
Indica se una sottostringa è presente o meno all'interno di una Slice
*/
func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

/*
Rimuove un elemento da una slice
*/
func RemoveElement(slice []string, remove string) []string {
	var i int
	for i = 0; i < len(slice); i++ {
		if slice[i] == remove {
			break
		}
	}

	slice[i] = slice[len(slice)-1]
	return slice[:len(slice)-1]
}

/*
Elimina tutti i file contenuti in una cartella specificata
*/
func ClearDir(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		return err
	}
	for _, file := range files {
		err = os.RemoveAll(file)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
Effettua l'hashing sha256 di una stringa
*/
func HashString(str string) [32]byte {
	return sha256.Sum256([]byte(str))
}

/*
Formatta la stringa per un Value in mongo
*/
func FormatValue(str string) string {
	if strings.Contains(str, "[") && strings.Contains(str, "]") {
		return str
	}
	return "[" + str + "]"
}

/*
Restituisce la stringa contenuta tra due parentesi quadre
*/
func RemoveBrackets(str string) string {
	if strings.Contains(str, "[") && strings.Contains(str, "]") {
		return str[1 : len(str)-1]
	}
	return str
}

/*
Appende un valore ad una stringa, mantenendo la lista contenuta tra due parentesi quadre
*/
func AppendValue(str string, arg1 string) string {
	temp := GetStringInBetween(str, "[", "]")
	append := temp + "," + arg1
	return FormatValue(append)
}

/*
Rimuove la porta dall'indirizzo di Chord Lookup, e aggiunge la porta per effettuare le RPC.
*/
func ParseAddrRPC(addr string) string {
	return RemovePort(addr) + RPC_PORT
}

/*
Rimuove la porta dall'indirizzo di Chord Lookup
*/
func RemovePort(addr string) string {
	return addr[:len(addr)-5]
}

/*
Esegue il clear del terminale
*/
func ClearScreen() {
	fmt.Print("\033[H\033[2J")
}
