package main

/*
   @Author: Bhaskar S
   @Blog:   https://www.polarsparc.com
   @Date:   22 Aug 2020
*/

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func compositeAddr(a1 string, a2 string) string {
	return a1 + ";" + a2
}

func copyBytes(t byte, buf *bytes.Buffer) []byte {
	res := make([]byte, buf.Len()+1)
	res[0] = t
	copy(res[1:], buf.Bytes())
	return res
}

func connRead(node RaftNode, conn net.Conn, source string) ([]byte, error) {
	to := conn.RemoteAddr().String()
	buf := make([]byte, MaxBufferSz)
	_, err := conn.Read(buf)
	if err != nil {
		fmt.Printf("-- [%s] %s - Could not read from endpoint [%s] - %s\n", node.Addr, source, to, err)
	//} else {
	//	fmt.Printf("++ [%s] Read %d bytes from endpoint [%s]\n", node.Addr, n, to)
	}
	return buf, err
}

func connWrite(node RaftNode, conn net.Conn, b []byte) error {
	to := conn.RemoteAddr().String()
	_, err := conn.Write(b)
	if err != nil {
		fmt.Printf("-- [%s] Could not send command to endpoint [%s] - %s\n", node.Addr, to, err)
	//} else {
	//	fmt.Printf("++ [%s] Sent %d bytes to endpoint [%s]\n", node.Addr, n, to)
	}
	return err
}

func nodeStateReset(leader bool, node *RaftNode) {
	if leader {
		node.ToLeader()
	} else {
		node.ToFollower()
	}
	node.ResetVoteCycles()
	node.VotesAck.Reset(node.Addr, node.Addr)
	for _, addr := range node.PeerAddr {
		node.VotesAck.Reset(addr, node.Addr)
	}
	node.ResetGrantedVote()
}

func sendGrantVote(node *RaftNode, vote bool, term int, index int, from string) {
	gv := NewGrantVote(vote, term, index, node.Addr)
	b := Serialize(*node, gv)
	if len(b) > 0 {
		if SendToPeer(*node, from, b) {
			if vote {
				node.SetGrantedVote()
			}
		}
	}
}

func sendHeartbeat(node *RaftNode) bool {
	hb := NewHeartbeat(node.CurrentTerm, node.CurrentIndex, node.LeaderAddr)
	b := Serialize(*node, hb)
	if len(b) > 0 {
		count := 1 // Self count
		for _, addr := range node.PeerAddr {
			if SendToPeer(*node, addr, b) {
				count++
			}
		}
		// No Quorum of Peers ???
		if count < node.QuorumCount() {
			fmt.Printf("-- [%s] Send Heartbeat - No QUORUM of Peer connections, RESET back to Follower\n",
				node.Addr)

			nodeStateReset(false, node)
			return false
		}
		node.SetLastHB(time.Now())

		//fmt.Printf("++ [%s] Send Heartbeat - Leader {Term: %d, Index: %d} to peers [%v]\n",
		//	node.Addr, node.CurrentTerm, node.CurrentIndex, node.PeerAddr)
	} else {
		return false
	}
	return true
}

// To be executed by the Leader - send older entries as AppendEntries to a specific Follower node
func syncPeer(node *RaftNode, index int, addr string) {
	entry, err := node.Log.Read(index)
	if err == nil {
		ae := NewAppendEntries(true, node.CurrentTerm, index, node.Addr, node.Addr, entry)
		b := Serialize(*node, ae)
		if len(b) > 0 {
			SendToPeer(*node, addr, b)
		}
	}
}

func sendCommitAck(node *RaftNode, ack bool, sync bool, term int, index int, client string) {
	ca := NewCommitAck(ack, sync, term, index, node.Addr, client)
	b := Serialize(*node, ca)
	if len(b) > 0 {
		SendToPeer(*node, node.LeaderAddr, b)
	}
}

func commitAckReset(node *RaftNode, client string) {
	node.CommitAck.Reset(client, node.Addr)
	for _, peer := range node.PeerAddr {
		node.CommitAck.Reset(client, peer)
	}
	node.CommitCycles.Reset(client)
}
