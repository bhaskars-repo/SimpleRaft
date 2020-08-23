package main

/*
   @Author: Bhaskar S
   @Blog:   https://www.polarsparc.com
   @Date:   22 Aug 2020
*/

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s ip-1:port-1 ip-2:port-2 ip-3:port-3 ...\n", os.Args[0])
		os.Exit(1)
	}

	n := NewRaftNode(os.Args[1:])

	ch := make(chan int)

	go StartServer(n)

	// Will block
	<-ch
}
