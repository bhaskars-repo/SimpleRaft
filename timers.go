package main

/*
   @Author: Bhaskar S
   @Blog:   https://www.polarsparc.com
   @Date:   22 Aug 2020
*/

import (
	"fmt"
	"time"
)

// Goroutine - Executed by all the nodes
func StartRaftTimer(node *RaftNode) {
	fmt.Printf("++ [%s] Election timer duration %d\n", node.Addr, node.Duration)

	_ = node.Log.Open() // Initialize the replication log

	// Election timer
	ed := time.Duration(node.Duration) * time.Millisecond
	et := time.NewTimer(ed)

	go func() {
		fmt.Printf("++ [%s] Election timer started @ %v\n", node.Addr, time.Now())

		for {
			select {
				case <-et.C: // Election timer event
					//fmt.Printf("++ [%s] ***** Election timer triggered @ %v\n", node.Addr, time.Now())

					HandleElectionTimeout(node)

					// Reset should always be invoked on stopped or expired channels
					et.Reset(ed)
				case msg := <-node.PeerC: // Handle peer events
					flag := false
					switch msg.(type) {
						case RequestVote: // From a Candidate peer
							//fmt.Printf("++ [%s] ***** Received RequestVote @ %v\n", node.Addr, time.Now())

							HandleRequestVote(node, msg.(RequestVote))
							flag = true
						case GrantVote: // From a Follower peer
							//fmt.Printf("++ [%s] ***** Received GrantVote @ %v\n", node.Addr, time.Now())

							HandleGrantVote(node, msg.(GrantVote))
							flag = true
						case Heartbeat: // From a Leader peer
							//fmt.Printf("++ [%s] ***** Received Heartbeat @ %v\n", node.Addr, time.Now())

							HandleHeartbeat(node, msg.(Heartbeat))
							flag = true
						case AppendEntries: // From a Leader peer
							//fmt.Printf("++ [%s] ***** Received AppendEntries @ %v\n", node.Addr, time.Now())

							HandleAppendEntries(node, msg.(AppendEntries))
							flag = true
						case CommitAck: // From a Follower peer
							//fmt.Printf("++ [%s] ***** Received CommitAck @ %v\n", node.Addr, time.Now())

							HandleCommitAck(node, msg.(CommitAck))
							flag = true
						default:
							flag = false
					}

					//fmt.Printf("++ [%s] Reset flag - %v @ %v\n", node.Addr, flag, time.Now())

					if flag {
						//fmt.Printf("++ [%s] ----- Resetting Election timer @ %v\n", node.Addr, time.Now())

						// Stop the current et and re-initialize a new et
						if !et.Stop() {
							<-et.C
						}
						et = time.NewTimer(ed)
					}
				case msg := <-node.LeaderC: // Handle client events
					switch msg.(type) {
						case ClientUpdate: // From a client
							//fmt.Printf("++ [%s] ***** Received ClientUpdate @ %v\n", node.Addr, time.Now())

							HandleClientUpdate(node, msg.(ClientUpdate))
						default:
							fmt.Printf("++ [%s] ***** Received Unknown Event @ %v\n", node.Addr, time.Now())
					}
			}
		}
	}()
}

// Goroutine - Only started on the Leader peer
func StartHeartbeatTimer(node *RaftNode) {
	fmt.Printf("++ [%s] Heartbeat timer duration %d\n", node.Addr, HeartbeatDuration)

	// Heartbeat timer
	hd := time.Duration(HeartbeatDuration) * time.Millisecond
	ht := time.NewTimer(hd)

	go func() {
		for {
			<-ht.C

			if node.IsLeader() {
				fmt.Printf("++ [%s] Heartbeat timer - {Term: %d, Index: %d, Committed: %d}\n",
					node.Addr, node.CurrentTerm, node.CurrentIndex, node.CommittedIndex)

				if !sendHeartbeat(node) {
					ht.Stop()
					break
				}
			} else {
				fmt.Printf("-- [%s] Heartbeat timer - NOT Leader, terminating Heartbeat timer @ %v\n",
					node.Addr, time.Now())

				ht.Stop()
				nodeStateReset(false, node)
				break
			}

			// Reset should always be invoked on stopped or expired channels
			ht.Reset(hd)
		}
	}()
}

func HandleElectionTimeout(node *RaftNode) {
	fmt.Printf("++ [%s] Election timeout - {Type: %s, IsLeaderOk: %v, GrantedVote: %v, LastHB: %v}\n",
		node.Addr, node.GetType(), node.isLeaderOk(), node.GrantedVote, node.LastHB)

	// No leader yet ???
	if node.IsFollower() && !node.isLeaderOk() && !node.GrantedVote {
		fmt.Printf("++ [%s] Election timeout - Follower ready to start Leader election\n", node.Addr)

		nodeStateReset(false, node)
		node.ToCandidate()
		node.VotesAck.IncrementYes(node.Addr, node.Addr) // Self vote YES

		rv := NewRequestVote(node.CurrentTerm+1, node.CurrentIndex, node.Addr)
		b := Serialize(*node, rv)
		if len(b) > 0 {
			count := 1 // Self count
			for _, addr := range node.PeerAddr {
				if SendToPeer(*node, addr, b) {
					count++
				}
			}
			// No Quorum of Peers ???
			if count < node.QuorumCount() {
				fmt.Printf("-- [%s] Election timeout - No QUORUM of Peer connections, RESET back to Follower\n",
					node.Addr)

				node.SetCurrentTerm(rv.Term)
				nodeStateReset(false, node)
			}
		}
	} else if node.IsCandidate() {
		yes := 1 // Self Ack YES
		no := 0
		for _, addr := range node.PeerAddr {
			yes += node.VotesAck.GetYes(addr, node.Addr)
			no += node.VotesAck.GetNo(addr, node.Addr)
		}

		fmt.Printf("++ [%s] Election timeout - Candidate {Votes: [YES: %d, NO: %d], Quorum: %d}\n",
			node.Addr, yes, no, node.QuorumCount())

		if yes >= node.QuorumCount() {
			fmt.Printf("++ [%s] Election timeout - Candidate PROMOTED to Leader\n", node.Addr)

			node.SetCurrentTerm(node.CurrentTerm + 1)
			node.SetLeaderAddr(node.Addr)
			nodeStateReset(true, node)
			sendHeartbeat(node)

			StartHeartbeatTimer(node)
		} else {
			if node.VotesCycles >= MaxWaitCycles {
				fmt.Printf("-- [%s] Election timeout - Maxed wait cycles - Election FAILED, " +
					"Candidate RESET to Follower\n", node.Addr)

				node.IncrDuration()
				nodeStateReset(false, node)
			} else {
				node.IncrVoteCycles()
			}
		}
	} else if node.IsLeader() && node.CurrentIndex != node.CommittedIndex {
		clients := node.ClientC.Clients()

		//fmt.Printf("++ [%s] Election timeout - Leader {Clients: %v}\n", node.Addr, clients)

		for _, addr := range clients {
			yes := 1 // Self Ack YES
			no := 0
			for _, peer := range node.PeerAddr {
				yes += node.CommitAck.GetYes(addr, peer)
				no += node.CommitAck.GetNo(addr, peer)
			}

			fmt.Printf("++ [%s] Election timeout - Leader {Client: [%s], Votes: [YES: %d, NO: %d], Quorum: %d}\n",
				node.Addr, addr, yes, no, node.QuorumCount())

			if yes >= node.QuorumCount() {
				fmt.Printf("++ [%s] Election timeout - Leader ACCEPTED update {Client: [%s], Index: %d}\n",
					node.Addr, addr, node.CurrentIndex)

				ua := NewUpdateAck(true, addr, node.Addr)
				node.ClientC[addr] <-ua
				commitAckReset(node, addr)
				node.SetCommittedIndex(node.CurrentIndex)
			} else {
				if node.CommitCycles.Count(addr) >= MaxWaitCycles {
					fmt.Printf("-- [%s] Election timeout - Maxed wait cycles, Leader REJECTED "+
						"update {Client: [%s], Index: %d, Committed: %d}\n",
						node.Addr, addr, node.CurrentIndex, node.CommittedIndex)

					ua := NewUpdateAck(false, addr, node.Addr)
					node.ClientC[addr] <-ua
					commitAckReset(node, addr)
				} else {
					node.CommitCycles.Increment(addr)
				}
			}
		}
	} else {
		if node.GrantedVote {
			nodeStateReset(false, node)
		}
	}
}

// To be executed by the Follower nodes
func HandleHeartbeat(node *RaftNode, msg Heartbeat) {
	fmt.Printf("++ [%s] Heartbeat event - {Term: %d, Index: %d, Leader: [%s]} => " +
		"{Term: %d, Index: %d, From: [%s]}\n", node.Addr, node.CurrentTerm, node.CurrentIndex, node.LeaderAddr,
		msg.Term, msg.Index, msg.From)

	if node.CurrentTerm <= msg.Term {
		if node.IsFollower() || node.IsCandidate() {
			node.SetLastHB(time.Now())
			if node.LeaderAddr != msg.From {
				node.SetCurrentTerm(msg.Term)
				node.SetLeaderAddr(msg.From)
				nodeStateReset(false, node)
			}
			if node.CurrentIndex < msg.Index {
				fmt.Printf("-- [%s] Heartbeat timeout - Node index BEHIND from [%s]\n", node.Addr, msg.From)

				sendCommitAck(node, false, true, msg.Term, node.CurrentIndex, msg.From)
			}
		}
	}
}
