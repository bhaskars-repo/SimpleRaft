package main

/*
   @Author: Bhaskar S
   @Blog:   https://www.polarsparc.com
   @Date:   22 Aug 2020
*/

import (
	"fmt"
	"net"
	"os"
	"time"
)

const MaxBufferSz int = 512

// Goroutine
func StartServer(node RaftNode) {
	fmt.Printf("++ [%s] Ready to start server on Raft node\n", node.Addr)

	listen, err := net.Listen("tcp", node.Addr)
	if err != nil {
		fmt.Printf("-- [%s] Could not start service on - %s\n", node.Addr, err)
		os.Exit(1)
	}

	StartRaftTimer(&node)

	fmt.Printf("++ [%s] Ready to accept remote connections\n", node.Addr)

	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Printf("-- [%s] Could not establish network connection - %s", node.Addr, err)
			continue
		}
		go handleRequest(&node, conn)
	}
}

func SendToPeer(node RaftNode, to string, b []byte) bool {
	conn, err := net.Dial("tcp", to)
	if err != nil {
		fmt.Printf("-- [%s] Could not establish connection to peer [%s] - %s\n", node.Addr, to, err)

		return false
	} else {
		defer conn.Close()
		err := connWrite(node, conn, b)
		if err != nil {
			return false
		}
	}
	return true
}

// Goroutine
func handleRequest(node *RaftNode, conn net.Conn) {
	buf, err := connRead(*node, conn, "Request")
	if err == nil {
		msg := Deserialize(*node, buf)
		switch msg.(type) {
			case Dummy:
				fmt.Printf("-- [%s] ***** Deserialized event *Dummy*\n", node.Addr)
				_ = conn.Close()
			case ClientUpdate:
				fmt.Printf("++ [%s] ClientUpdate event from %s\n", node.Addr, conn.RemoteAddr().String())

				go handleClient(node, conn, msg)
			default:
				node.PeerC <-msg
				_ = conn.Close()
		}
	}
}

// Goroutine - Keep the client connection open and handle future updates from the client
func handleClient(node *RaftNode, conn net.Conn, msg interface{}) {
	defer conn.Close()
	ca := msg.(ClientUpdate)
	node.ClientC[ca.Addr] = make(chan interface{})
	for {
		//fmt.Printf("++ [%s] Client - Update event from [%s]\n", node.Addr, ca.Addr)

		node.LeaderC <-msg

		//fmt.Printf("++ [%s] Client - Update from [%s] sent to Leader channel\n", node.Addr, ca.Addr)

		evt := <-node.ClientC[ca.Addr]

		ua := evt.(UpdateAck)

		fmt.Printf("++ [%s] Client - UpdateAck event from [%s] {Ack: %v, Addr: %s}\n",
			node.Addr, ua.From, ua.Ack, ua.Addr)

		// Send response to client
		b := Serialize(*node, ua)
		if len(b) > 0 {
			err := connWrite(*node, conn, b)
			if err != nil {
				break
			}
		} else {
			fmt.Printf("-- [%s] Client - Serialization error, disconnect client [%s]\n", node.Addr, ua.Addr)
			break
		}

		if !ua.Ack {
			fmt.Printf("-- [%s] Client - Leader Update REJECT, disconnect client [%s]\n", node.Addr, ua.Addr)

			// Give a little time for client to read
			time.Sleep(50 * time.Millisecond)
			break
		}

		// Read next update from client
		buf, err := connRead(*node, conn, "Client")
		if err == nil {
			msg = Deserialize(*node, buf)
			ca = msg.(ClientUpdate)
		} else {
			fmt.Printf("-- [%s] Client - Network read error, disconnect client [%s]\n", node.Addr, ua.Addr)

			break
		}
	}
}
