package api

import (
	"JDSys/node/chord/internal"
	"JDSys/utils"
	"log"

	"github.com/golang/protobuf/proto"
)

//lookupMsg constructs a message to perform the lookup of a key and returns the
//marshalled protocol buffer
func getfingersMsg() []byte {

	msg := new(internal.NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(internal.ChordMessage)
	command := internal.ChordMessage_Command(internal.ChordMessage_Command_value["GetFingers"])
	chordMsg.Cmd = &command
	chorddata, err := proto.Marshal(chordMsg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	msg.Msg = proto.String(string(chorddata))

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

func sendfingersMsg(fingers []NodeInfo) []byte {

	msg := new(internal.NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(internal.ChordMessage)
	command := internal.ChordMessage_Command(internal.ChordMessage_Command_value["GetFingers"])
	chordMsg.Cmd = &command
	sfMsg := new(internal.SendFingersMessage)
	for _, finger := range fingers {
		if !finger.zero() {
			fingerMsg := new(internal.FingerMessage)
			fingerMsg.Id = proto.String(string(finger.id[:32]))
			fingerMsg.Address = proto.String(finger.ipaddr)
			sfMsg.Fingers = append(sfMsg.Fingers, fingerMsg)
		}
	}
	chordMsg.Sfmsg = sfMsg
	chorddata, err := proto.Marshal(chordMsg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	msg.Msg = proto.String(string(chorddata))

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//getidMsg constructs a message to ask a server for its chord id
func getidMsg() []byte {

	msg := new(internal.NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(internal.ChordMessage)
	command := internal.ChordMessage_Command(internal.ChordMessage_Command_value["GetId"])
	chordMsg.Cmd = &command
	chorddata, err := proto.Marshal(chordMsg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	msg.Msg = proto.String(string(chorddata))

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//sendidMsg constructs a message to ask a server for its chord id
func sendidMsg(id []byte) []byte {

	msg := new(internal.NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(internal.ChordMessage)
	command := internal.ChordMessage_Command(internal.ChordMessage_Command_value["GetId"])
	chordMsg.Cmd = &command
	sidMsg := new(internal.SendIdMessage)
	sidMsg.Id = proto.String(string(id))
	chordMsg.Sidmsg = sidMsg
	chorddata, err := proto.Marshal(chordMsg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	msg.Msg = proto.String(string(chorddata))

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

func getpredMsg() []byte {
	msg := new(internal.NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(internal.ChordMessage)
	command := internal.ChordMessage_Command(internal.ChordMessage_Command_value["GetPred"])
	chordMsg.Cmd = &command
	chorddata, err := proto.Marshal(chordMsg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	msg.Msg = proto.String(string(chorddata))

	data, err := proto.Marshal(msg)

	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	return data
}

func sendpredMsg(finger NodeInfo) []byte {
	msg := new(internal.NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(internal.ChordMessage)
	command := internal.ChordMessage_Command(internal.ChordMessage_Command_value["GetPred"])
	chordMsg.Cmd = &command
	pMsg := new(internal.PredMessage)
	fingerMsg := new(internal.FingerMessage)
	fingerMsg.Id = proto.String(string(finger.id[:32]))
	fingerMsg.Address = proto.String(finger.ipaddr)
	pMsg.Pred = fingerMsg
	chordMsg.Cpmsg = pMsg

	chorddata, err := proto.Marshal(chordMsg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	msg.Msg = proto.String(string(chorddata))

	data, err := proto.Marshal(msg)

	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

func claimpredMsg(finger NodeInfo) []byte {
	msg := new(internal.NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(internal.ChordMessage)
	command := internal.ChordMessage_Command(internal.ChordMessage_Command_value["ClaimPred"])
	chordMsg.Cmd = &command
	predMsg := new(internal.PredMessage)
	fingerMsg := new(internal.FingerMessage)
	fingerMsg.Id = proto.String(string(finger.id[:32]))
	fingerMsg.Address = proto.String(finger.ipaddr)
	predMsg.Pred = fingerMsg
	chordMsg.Cpmsg = predMsg

	chorddata, err := proto.Marshal(chordMsg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	msg.Msg = proto.String(string(chorddata))

	data, err := proto.Marshal(msg)

	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//pingMsg constructs a message to ping a server
func pingMsg() []byte {

	msg := new(internal.NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(internal.ChordMessage)
	command := internal.ChordMessage_Command(internal.ChordMessage_Command_value["Ping"])
	chordMsg.Cmd = &command
	chorddata, err := proto.Marshal(chordMsg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	msg.Msg = proto.String(string(chorddata))

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//pongMsg constructs a message to reply to a ping
func pongMsg() []byte {

	msg := new(internal.NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(internal.ChordMessage)
	command := internal.ChordMessage_Command(internal.ChordMessage_Command_value["Pong"])
	chordMsg.Cmd = &command
	chorddata, err := proto.Marshal(chordMsg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	msg.Msg = proto.String(string(chorddata))

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

func getsuccessorsMsg() []byte {
	msg := new(internal.NetworkMessage)
	msg.Proto = proto.Uint32(1)
	chordMsg := new(internal.ChordMessage)
	command := internal.ChordMessage_Command(internal.ChordMessage_Command_value["GetSucc"])
	chordMsg.Cmd = &command
	chorddata, err := proto.Marshal(chordMsg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	msg.Msg = proto.String(string(chorddata))

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data

}

func nullMsg() []byte {
	msg := new(internal.NetworkMessage)
	msg.Proto = proto.Uint32(1)

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	return data
}

//parseMessage takes as input an unmarshalled protocol buffer and
//performs actions based on what the message contains.
func (node *ChordNode) parseMessage(data []byte, c chan []byte) {

	msg := new(internal.NetworkMessage)

	err := proto.Unmarshal(data, msg)
	checkError(err)
	if err != nil {
		utils.PrintTs("Uh oh in network parse message of node " + node.ipaddr)
		return
	}

	protocol := msg.GetProto()
	if protocol != 1 {
		if app, ok := node.applications[byte(protocol)]; ok {
			c <- app.Message([]byte(msg.GetMsg()))
		}
		return
	}

	chorddata := []byte(msg.GetMsg())
	chordmsg := new(internal.ChordMessage)
	err = proto.Unmarshal(chorddata, chordmsg)
	checkError(err)
	if err != nil {
		utils.PrintTs("Uh oh (1) in chord parse message of node " + node.ipaddr)
		return
	}

	cmd := int32(chordmsg.GetCmd())
	switch {
	case cmd == internal.ChordMessage_Command_value["Ping"]:
		c <- pongMsg()
		return
	case cmd == internal.ChordMessage_Command_value["GetPred"]:
		node.request <- request{false, false, -1}
		pred := <-node.finger
		if pred.zero() {
			c <- nullMsg()
		} else {
			c <- sendpredMsg(pred) //node.predecessor)
		}
		return
	case cmd == internal.ChordMessage_Command_value["GetId"]:
		c <- sendidMsg(node.id[:32])
		return
	case cmd == internal.ChordMessage_Command_value["GetFingers"]:
		table := make([]NodeInfo, 32*8+1)
		for i := range table {
			node.request <- request{false, false, i}
			f := <-node.finger
			table[i] = f
		}

		c <- sendfingersMsg(table)
		return
	case cmd == internal.ChordMessage_Command_value["ClaimPred"]:
		//extract finger
		newPred, err := parseFinger(data)
		checkError(err)
		if err != nil {
			c <- nullMsg()
			break
		}
		node.request <- request{false, false, -1}
		pred := <-node.finger

		if pred.zero() || InRange(newPred.id, pred.id, node.id) {
			go node.notify(newPred)
		}
		c <- nullMsg()
		//update finger table
		return
	case cmd == internal.ChordMessage_Command_value["GetSucc"]:
		table := make([]NodeInfo, 32*8)
		for i := range table {
			node.request <- request{false, true, i}
			f := <-node.finger
			table[i] = f
		}

		c <- sendfingersMsg(table)
		return

	}
	utils.PrintTs("No matching commands.\n")
}

//parseFingers can be called to return a finger table from a received
//parseFingers can be called to return a finger table from a received
//message after a getfingers call.
func parseFingers(data []byte) (ft []NodeInfo, err error) {
	msg := new(internal.NetworkMessage)
	err = proto.Unmarshal(data, msg)
	if msg.GetProto() != 1 {
		return
	}
	chorddata := []byte(msg.GetMsg())
	chordmsg := new(internal.ChordMessage)
	err = proto.Unmarshal(chorddata, chordmsg)
	checkError(err)
	if err != nil {
		utils.PrintTs("Uh oh (2) in chord parse message.\n")
		return
	}
	if chordmsg == nil {
		return
	}
	sfmsg := chordmsg.GetSfmsg()
	fingers := sfmsg.GetFingers()
	prevfinger := new(NodeInfo)
	for _, finger := range fingers {
		newfinger := new(NodeInfo)
		copy(newfinger.id[:], []byte(*finger.Id))
		newfinger.ipaddr = *finger.Address
		if !newfinger.zero() && newfinger.ipaddr != prevfinger.ipaddr {
			ft = append(ft, *newfinger)
		}
		*prevfinger = *newfinger
	}
	return
}

func parseFinger(data []byte) (f NodeInfo, err error) {
	msg := new(internal.NetworkMessage)
	err = proto.Unmarshal(data, msg)
	checkError(err)
	if err != nil {
		utils.PrintTs("Uh oh (3) in network parse message.\n")
		return
	}

	if msg.GetMsg() == "" { //then received null msg instead. return nil
		return
	}

	chorddata := []byte(msg.GetMsg())
	chordmsg := new(internal.ChordMessage)
	err = proto.Unmarshal(chorddata, chordmsg)
	checkError(err)
	if err != nil {
		utils.PrintTs("Uh oh (3) in chord parse message.\n")
		return
	}

	cpmsg := chordmsg.GetCpmsg()
	finger := cpmsg.GetPred()
	copy(f.id[:], []byte(*finger.Id))
	f.ipaddr = *finger.Address

	return
}

func parseId(data []byte) (id [32]byte, err error) {
	msg := new(internal.NetworkMessage)
	err = proto.Unmarshal(data, msg)
	if msg.GetProto() != 1 {
		return
	}

	chorddata := []byte(msg.GetMsg())
	chordmsg := new(internal.ChordMessage)
	err = proto.Unmarshal(chorddata, chordmsg)
	checkError(err)
	if err != nil {
		utils.PrintTs("Uh oh (4) in chord parse message.\n")
		return
	}

	if chordmsg == nil { //then received null msg instead. return nil
		return
	}

	idmsg := chordmsg.GetSidmsg()
	arr := []byte(idmsg.GetId())
	copy(id[:], arr[:32])
	return
}

func parsePong(data []byte) (success bool, err error) {

	msg := new(internal.NetworkMessage)
	err = proto.Unmarshal(data, msg)
	checkError(err)
	if err != nil {
		return false, err
	}

	if msg.GetProto() != 1 {
		utils.PrintTs("Something went wrong!\n")
		return
	}

	chorddata := []byte(msg.GetMsg())
	chordmsg := new(internal.ChordMessage)
	err = proto.Unmarshal(chorddata, chordmsg)
	checkError(err)
	if err != nil {
		utils.PrintTs("Uh oh (5) in chord parse message.\n")
		return
	}

	if chordmsg == nil { //then received null msg instead. return nil
		return
	}

	command := int32(chordmsg.GetCmd())
	if command == internal.ChordMessage_Command_value["Pong"] {
		success = true
	} else {
		success = false
	}

	return
}
