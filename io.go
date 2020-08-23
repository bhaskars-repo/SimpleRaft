package main

/*
   @Author: Bhaskar S
   @Blog:   https://www.polarsparc.com
   @Date:   22 Aug 2020
*/

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

const (
	RVType byte = 0x01
	GVType byte = 0x02
	HBType byte = 0x03
	AEType byte = 0x04
	CAType byte = 0x05
	CUType byte = 0x06
	UAType byte = 0x07
)

func Serialize(node RaftNode, t interface{}) []byte {
	buf := bytes.NewBuffer([]byte{})
	enc := gob.NewEncoder(buf)

	switch v := t.(type) {
		case RequestVote:
			//fmt.Printf("++ [%s] v is of type RequestVote: %v\n", node.Addr, v)

			err := enc.Encode(v)
			if err != nil {
				fmt.Printf("-- [%s] Could not encode RequestVote - %s", node.Addr, err)
			} else {
				return copyBytes(RVType, buf)
			}
		case GrantVote:
			//fmt.Printf("++ [%s] v is of type GrantVote: %v\n", node.Addr, v)

			err := enc.Encode(v)
			if err != nil {
				fmt.Printf("-- [%s] Could not encode GrantVote - %s", node.Addr, err)
			} else {
				return copyBytes(GVType, buf)
			}
		case Heartbeat:
			//fmt.Printf("++ [%s] v is of type Heartbeat: %v\n", node.Addr, v)

			err := enc.Encode(v)
			if err != nil {
				fmt.Printf("-- [%s] Could not encode Heartbeat - %s", node.Addr, err)
			} else {
				return copyBytes(HBType, buf)
			}
		case AppendEntries:
			//fmt.Printf("++ [%s] v is of type AppendEntries: %v\n", node.Addr, v)

			err := enc.Encode(v)
			if err != nil {
				fmt.Printf("-- [%s] Could not encode AppendEntries - %s", node.Addr, err)
			} else {
				return copyBytes(AEType, buf)
			}
		case CommitAck:
			//fmt.Printf("++ [%s] v is of type CommitAck: %v\n", node.Addr, v)

			err := enc.Encode(v)
			if err != nil {
				fmt.Printf("-- [%s] Could not encode CommitAck - %s", node.Addr, err)
			} else {
				return copyBytes(CAType, buf)
			}
		case ClientUpdate:
			//fmt.Printf("++ [%s] v is of type ClientUpdate: %v\n", node.Addr, v)

			err := enc.Encode(v)
			if err != nil {
				fmt.Printf("-- [%s] Could not encode ClientUpdate - %s", node.Addr, err)
			} else {
				return copyBytes(CUType, buf)
			}
		case UpdateAck:
			//fmt.Printf("++ [%s] v is of type UpdateAck: %v\n", node.Addr, v)

			err := enc.Encode(v)
			if err != nil {
				fmt.Printf("-- [%s] Could not encode UpdateAck - %s", node.Addr, err)
			} else {
				return copyBytes(UAType, buf)
			}
	}

	return []byte{}
}

func Deserialize(node RaftNode, b []byte) interface{} {
	dec := gob.NewDecoder(bytes.NewBuffer(b[1:]))

	switch b[0] {
		case RVType:
			t1 := RequestVote{}
			err := dec.Decode(&t1)
			if err != nil {
				fmt.Printf("-- [%s] Could not decode RequestVote - %s", node.Addr, err)
			//} else {
			//	fmt.Printf("++ [%s] Decoded RequestVote: %v\n", node.Addr, t1)
			}
			return t1
		case GVType:
			t2 := GrantVote{}
			err := dec.Decode(&t2)
			if err != nil {
				fmt.Printf("-- [%s] Could not decode GrantVote - %s", node.Addr, err)
			//} else {
			//	fmt.Printf("++ [%s] Decoded GrantVote: %v\n", node.Addr, t2)
			}
			return t2
		case HBType:
			t3 := Heartbeat{}
			err := dec.Decode(&t3)
			if err != nil {
				fmt.Printf("-- [%s] Could not decode Heartbeat - %s", node.Addr, err)
			//} else {
			//	fmt.Printf("++ [%s] Decoded Heartbeat: %v\n", node.Addr, t3)
			}
			return t3
		case AEType:
			t4 := AppendEntries{}
			err := dec.Decode(&t4)
			if err != nil {
				fmt.Printf("-- [%s] Could not decode AppendEntries - %s", node.Addr, err)
			//} else {
			//	fmt.Printf("++ [%s] Decoded AppendEntries: %v\n", node.Addr, t4)
			}
			return t4
		case CAType:
			t5 := CommitAck{}
			err := dec.Decode(&t5)
			if err != nil {
				fmt.Printf("-- [%s] Could not decode CommitAck - %s", node.Addr, err)
			//} else {
			//	fmt.Printf("++ [%s] Decoded CommitAck: %v\n", node.Addr, t5)
			}
			return t5
		case CUType:
			t6 := ClientUpdate{}
			err := dec.Decode(&t6)
			if err != nil {
				fmt.Printf("-- [%s] Could not decode ClientUpdate - %s", node.Addr, err)
			//} else {
			//	fmt.Printf("++ [%s] Decoded ClientUpdate: %v\n", node.Addr, t6)
			}
			return t6
		case UAType:
			t7 := UpdateAck{}
			err := dec.Decode(&t7)
			if err != nil {
				fmt.Printf("-- [%s] Could not decode UpdateAck - %s", node.Addr, err)
			//} else {
			//	fmt.Printf("++ [%s] Decoded UpdateAck: %v\n", node.Addr, t7)
			}
			return t7
	}

	return Dummy{}
}
