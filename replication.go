package main

/*
   @Author: Bhaskar S
   @Blog:   https://www.polarsparc.com
   @Date:   22 Aug 2020
*/

import "fmt"

// To handle updates from a client
func HandleClientUpdate(node *RaftNode, msg ClientUpdate) {
	if node.IsLeader() {
		fmt.Printf("++ [%s] ClientUpdate event - {Addr: [%s], Entry: %s, Time: %v}\n",
			node.Addr, msg.Addr, msg.Entry, msg.TStamp)

		commitAckReset(node, msg.Addr)

		err := node.Log.Write(node.CurrentIndex+1, msg.Entry)
		if err == nil {
			ae := NewAppendEntries(false, node.CurrentTerm, node.CurrentIndex+1, node.Addr, msg.Addr, msg.Entry)
			b := Serialize(*node, ae)
			if len(b) > 0 {
				count := 1 // Self count YES
				for _, addr := range node.PeerAddr {
					if SendToPeer(*node, addr, b) {
						count++
					}
				}
				// No Quorum ???
				if count < node.QuorumCount() {
					fmt.Printf("-- [%s] ClientUpdate - NO Quorum, REJECT Update from Client [%s]\n", node.Addr, msg.Addr)

					ua := NewUpdateAck(false, msg.Addr, node.Addr)
					node.ClientC[msg.Addr] <-ua
				}
				node.SetCurrentIndex(node.CurrentIndex+1)
			}
		}
	} else {
		fmt.Printf("-- [%s] ClientUpdate - NOT Leader, REJECT Update from Client [%s]\n", node.Addr, msg.Addr)

		ua := NewUpdateAck(false, msg.Addr, node.Addr)
		node.ClientC[msg.Addr] <-ua
	}
}

// To be executed by the Follower nodes
func HandleAppendEntries(node *RaftNode, msg AppendEntries) {
	fmt.Printf("++ [%s] AppendEntries event - {Term: %d, Index: %d, Leader: [%s]} => " +
		"{Term: %d, Index: %d, Client: [%s], Entry: %s}\n", node.Addr, node.CurrentTerm, node.CurrentIndex,
		node.LeaderAddr, msg.Term, msg.Index, msg.Client, msg.Entry)

	if node.IsFollower() && node.CurrentTerm == msg.Term && node.LeaderAddr == msg.Leader {
		if node.CurrentIndex+1 <= msg.Index {
			if node.CurrentIndex+1 == msg.Index {
				ack := false
				sync := false

				if msg.Sync {
					fmt.Printf("++ [%s] AppendEntries - SYNC accepted {Index: %d} with {Entry: %s}\n",
						node.Addr, msg.Index, msg.Entry)

					sync = true
				} else {
					fmt.Printf("++ [%s] AppendEntries - UPDATE accepted {Index: %d} with {Entry: %s}\n",
						node.Addr, msg.Index, msg.Entry)

					ack = true
				}

				err := node.Log.Write(msg.Index, msg.Entry)
				if err == nil {
					node.SetCurrentIndex(msg.Index)
					sendCommitAck(node, ack, sync, msg.Term, msg.Index, msg.Client)
				}
			} else {
				fmt.Printf("-- [%s] AppendEntries - Node index BEHIND, REJECT entry from [%s]\n",
					node.Addr, msg.Client)

				sendCommitAck(node, false, true, msg.Term, node.CurrentIndex, msg.Client)
			}
		} else {
			// Did the CommitAck not reach the Leader node ???
			if node.CurrentIndex == msg.Index {
				sendCommitAck(node, true, false, msg.Term, msg.Index, msg.Client)
			} else {
				fmt.Printf("-- [%s] AppendEntries - Node index AHEAD, REJECT entry from [%s]\n",
					node.Addr, msg.Client)

				sendCommitAck(node, false, false, msg.Term, node.CurrentIndex, msg.Client)
			}
		}
	} else {
		fmt.Printf("-- [%s] AppendEntries - Node NOT in sync, REJECT entry from [%s]\n", node.Addr, msg.Client)

		sendCommitAck(node, false, false, node.CurrentTerm, node.CurrentIndex, msg.Client)
	}
}

// To be executed by the Leader node
func HandleCommitAck(node *RaftNode, msg CommitAck) {
	fmt.Printf("++ [%s] CommitAck event - {Term: %d, Index: %d} => {Ack: %v, Term: %d, Index: %d, " +
		"Peer: [%s], Client: [%s]}\n",
		node.Addr, node.CurrentTerm, node.CurrentIndex, msg.Ack, msg.Term, msg.Index, msg.Addr, msg.Client)

	if node.IsLeader() {
		// Is Follower behind ???
		if msg.Sync && node.CurrentIndex > msg.Index {
			syncPeer(node, msg.Index+1, msg.Addr)
		} else if node.CommittedIndex < node.CurrentIndex {
			if msg.Ack {
				node.CommitAck.IncrementYes(msg.Client, msg.Addr)
			} else {
				node.CommitAck.IncrementNo(msg.Client, msg.Addr)
			}

			yes := 1 // Self Ack YES
			no := 0
			for _, peer := range node.PeerAddr {
				yes += node.CommitAck.GetYes(msg.Client, peer)
				no += node.CommitAck.GetNo(msg.Client, peer)
			}

			if yes >= node.QuorumCount() {
				fmt.Printf("++ [%s] CommitAck - Leader ACCEPTED update {Client: [%s], Index: %d}\n",
					node.Addr, msg.Client, node.CurrentIndex)

				ua := NewUpdateAck(true, msg.Client, node.Addr)
				node.ClientC[msg.Client] <-ua
				commitAckReset(node, msg.Client)
				node.SetCommittedIndex(node.CurrentIndex)
			} else {
				// Did we not get all the votes ???
				if (yes + no) >= node.AllCount() {
					fmt.Printf("-- [%s] CommitAck - NO Quorum, Leader REJECTED update from client [%s]\n",
						node.Addr, msg.Client)

					ua := NewUpdateAck(false, msg.Client, node.Addr)
					node.ClientC[msg.Client] <-ua
					commitAckReset(node, msg.Client)
				}
			}
		}
	} else {
		fmt.Printf("-- [%s] CommitAck - NOT Leader, Ignore CommitAck from [%s]\n", node.Addr, msg.Client)
	}
}
