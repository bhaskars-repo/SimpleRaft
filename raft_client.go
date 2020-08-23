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

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s leader-ip-1:leader-port-1\n", os.Args[0])
		os.Exit(1)
	}

	quotes := []string{
		"It does not matter how slowly you go as long as you do not stop",
		"The secret of getting ahead is getting started",
		"Be kind whenever possible. It is always possible",
		"It always seems impossible until it's done",
		"If you're going through hell, keep going",
		"Quality is not an act, it is a habit",
		"Setting goals is the first step in turning the invisible into the visible",
		"Things do not happen. Things are made to happen",
		"There's a way to do it better - find it",
		"Small deeds done are better than great deeds planned",
	}

	to := os.Args[1]

	fmt.Printf("++ Ready to connect to Raft Leader node [%s]\n", to)

	conn, err := net.Dial("tcp", os.Args[1])
	if err != nil {
		fmt.Printf("-- Could not establish connection to Leader [%s] - %s\n", to, err)
		os.Exit(1)
	} else {
		defer conn.Close()
		sleep := time.Duration(10)
		node := RaftNode{Addr: conn.LocalAddr().String()}
		for _, entry := range quotes {
			ce := NewClientUpdate(node.Addr, entry)
			b := Serialize(node, ce)
			if len(b) > 0 {
				// Send
				n, err := conn.Write(b)
				if err != nil {
					fmt.Printf("-- [%s] Could not send command to endpoint [%s] - %s\n", node.Addr, to, err)
					os.Exit(1)
				} else {
					fmt.Printf("++ [%s] Sent %d bytes to endpoint [%s]\n", node.Addr, n, to)

					// Receive
					buf := make([]byte, MaxBufferSz)
					n, err = conn.Read(buf)
					if err != nil {
						fmt.Printf("-- [%s] Could not read from endpoint [%s] - %s\n", node.Addr, to, err)
						os.Exit(1)
					} else {
						fmt.Printf("++ [%s] Read %d bytes from endpoint [%s]\n", node.Addr, n, to)

						msg := Deserialize(node, buf)
						ua := msg.(UpdateAck)
						if !ua.Ack {
							fmt.Printf("-- [%s] Update *FAILED* to replicate via Leader at [%s]\n", node.Addr, to)
							break
						} else {
							fmt.Printf("++ [%s] Update REPLICATED via Leader at [%s]\n", node.Addr, to)
						}
					}

					fmt.Printf("++ Ready to sleep for %d seconds [%s]\n", sleep, to)

					// Wait few seconds
					time.Sleep(sleep * time.Second)
				}
			}
		}
	}
}
