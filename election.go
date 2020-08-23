package main

/*
   @Author: Bhaskar S
   @Blog:   https://www.polarsparc.com
   @Date:   22 Aug 2020
*/

import "fmt"

// To be executed by all Peer nodes
func HandleRequestVote(node *RaftNode, msg RequestVote) {
	fmt.Printf("++ [%s] RequestVote event - {Term: %d, Index: %d, Leader: [%s]} => " +
		"{Term: %d, Index: %d, From: [%s]}\n", node.Addr, node.CurrentTerm, node.CurrentIndex, node.LeaderAddr,
		msg.Term, msg.Index, msg.From)

	if node.IsFollower() {
		// Election Safety - we will only let a Follower that is up-to-date to vote
		if node.CurrentTerm <= msg.Term && node.CurrentIndex <= msg.Index {
			fmt.Printf("++ [%s] RequestVote - Follower will GrantVote to [%s]\n", node.Addr, msg.From)

			node.SetCurrentTerm(msg.Term)
			node.SetCurrentIndex(msg.Index)
			node.SetLeaderAddr(msg.From)
			sendGrantVote(node, true, msg.Term, msg.Index, msg.From)
		} else {
			fmt.Printf("-- [%s] RequestVote - Follower out-of-sync {CurrentTerm: %d, Term:%d, " +
				"CurrentIndex: %d, Index: %d}, DENY GrantVote to [%s]\n",
				node.Addr, node.CurrentTerm, msg.Term, node.CurrentIndex, msg.Index, msg.From)

			sendGrantVote(node, false, node.CurrentTerm, node.CurrentIndex, msg.From)
		}
	} else { // Could be Candidate or Leader
		fmt.Printf("-- [%s] RequestVote - NOT Follower, DENY GrantVote to [%s]\n", node.Addr, msg.From)

		sendGrantVote(node, false, msg.Term, msg.Index, msg.From)
	}
}

// To be executed by the Candidate node
func HandleGrantVote(node *RaftNode, msg GrantVote) {
	fmt.Printf("++ [%s] GrantVote event - {Term: %d, Index: %d, Leader: [%s]} => " +
		"{Vote: %v, Term: %d, Index: %d, From: [%s]}\n", node.Addr, node.CurrentTerm, node.CurrentIndex,
		node.LeaderAddr, msg.Vote, msg.Term, msg.Index, msg.From)

	if node.IsCandidate() {
		// Election Safety - we will only let a Candidate that is up-to-date to promote itself to a Leader
		// if there is a quorum of approved votes
		if node.CurrentTerm <= msg.Term && node.CurrentIndex <= msg.Index {
			if msg.Vote {
				node.VotesAck.IncrementYes(msg.From, node.Addr) // Peer vote YES
			} else {
				node.VotesAck.IncrementNo(msg.From, node.Addr) // Peer vote NO
			}

			yes := 1 // Self Ack YES
			no := 0
			for _, addr := range node.PeerAddr {
				yes += node.VotesAck.GetYes(addr, node.Addr)
				no += node.VotesAck.GetNo(addr, node.Addr)
			}

			fmt.Printf("++ [%s] GrantVote - Candidate {Votes: [YES: %d, NO: %d], Quorum: %d}\n",
				node.Addr, yes, no, node.QuorumCount())

			if yes >= node.QuorumCount() {
				fmt.Printf("++ [%s] GrantVote - Candidate PROMOTED to Leader\n", node.Addr)

				node.SetCurrentTerm(msg.Term)
				node.SetLeaderAddr(node.Addr)
				nodeStateReset(true, node)
				sendHeartbeat(node)

				StartHeartbeatTimer(node)
			} else {
				// Did we get all the votes ???
				if (yes + no) >= node.AllCount() {
					fmt.Printf("-- [%s] GrantVote - No QUORUM, Candidate RESET to Follower\n", node.Addr)

					node.SetCurrentTerm(msg.Term)
					nodeStateReset(false, node)
				}
			}
		} else {
			fmt.Printf("-- [%s] GrantVote - Candidate Out-of-sync, RESET to Follower\n", node.Addr)

			node.SetCurrentTerm(msg.Term)
			nodeStateReset(false, node)
		}
	} else {
		// Only apply for Follower
		if node.IsFollower() {
			fmt.Printf("-- [%s] GrantVote - Is Follower, IGNORE GrantVote from [%s]\n", node.Addr, msg.From)

			node.SetCurrentTerm(msg.Term)
			nodeStateReset(false, node)
		}
	}
}
