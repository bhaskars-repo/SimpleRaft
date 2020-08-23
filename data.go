package main

/*
   @Author: Bhaskar S
   @Blog:   https://www.polarsparc.com
   @Date:   22 Aug 2020
*/

import (
	"math/rand"
	"time"
)

const MinElectionDuration int = 1150
const MaxElectionDuration int = 1300
const HeartbeatDuration   int = 1000
const DurationIncrRange   int = 100
const MaxWaitCycles       int = 3

type Dummy struct {
}

/*
   AckCounter is used for tracking acknowledgements from the nodes in the Raft cluster.
   It is used in the two cases - Leader Election and Log Replication
 */

type YesNo struct {
	Yes int
	No  int
}

type AckCounter map[string]*YesNo

func (c AckCounter) Reset(from string, to string) {
	k := compositeAddr(from, to)
	c[k] = &YesNo{0, 0}
}

func (c AckCounter) GetYes(from string, to string) int {
	k := compositeAddr(from, to)
	if v, ok := c[k]; ok {
		return v.Yes
	} else {
		c[k] = &YesNo{0, 0}
	}
	return 0
}

func (c AckCounter) GetNo(from string, to string) int {
	k := compositeAddr(from, to)
	if v, ok := c[k]; ok {
		return v.No
	} else {
		c[k] = &YesNo{0, 0}
	}
	return 0
}

func (c AckCounter) IncrementYes(from string, to string) {
	k := compositeAddr(from, to)
	if v, ok := c[k]; ok {
		v.Yes++
	} else {
		c[k] = &YesNo{1, 0}
	}
}

func (c AckCounter) IncrementNo(from string, to string) {
	k := compositeAddr(from, to)
	if v, ok := c[k]; ok {
		v.No++
	} else {
		c[k] = &YesNo{0, 1}
	}
}

/*
   UpdateAckCycles is used to keep a count of the number of election timeouts to wait (wait cycles). When a
   message is send to the nodes in the cluster there could be delays as well as losses. How long should we
   wait before we say - times up, not heard. Each election timeout is one cycle. In our implementation we
   typically wait for 3 cycles before we give up. This is for each client that sends updates to the Leader
 */

type UpdateAckCycles map[string]int

func (u UpdateAckCycles) Count(addr string) int {
	return u[addr]
}

func (u UpdateAckCycles) Increment(addr string) {
	u[addr]++
}

func (u UpdateAckCycles) Reset(addr string) {
	u[addr] = 0
}

/*
   ClientChannel is used to map a golang channel with each client
 */

type ClientChannel map[string]chan interface{}

func (c ClientChannel) Clients() []string {
	cs := make([]string, 0)
	for k := range c {
		cs = append(cs, k)
	}
	return cs
}

/*
   AppendLog is the generic interface representing a Log Replication target. For our demonstration, we
   use MemLog which is a simple in-memory cache of the updates from the client
 */

type AppendLog interface {
	Open() error
	Read(index int) (string, error)
	Write(index int, entry string) error
	Close() error
}

/*
   RaftNode encapsulates the state of a Raft node
 */

type RaftNodeType byte

const (
	Follower RaftNodeType = 0x01
	Candidate             = 0x02
	Leader                = 0x03
)

type RaftNode struct {
	Type           RaftNodeType        // Type of the node - Follower, Candidate, or Leader
	GrantedVote    bool                // Flag that indicates if this node granted a vote to a Candidate
	CurrentTerm    int                 // Tracks the current election term of each node
	CurrentIndex   int                 // Tracks the current index on each node
	CommittedIndex int                 // Tracks the index committed on the Leader
	Duration       int                 // Represents the random duration for election timeout
	VotesCycles    int                 // Count of wait cycles during Leader Election
	Addr           string              // Network endpoint of this node
	LeaderAddr     string              // Network endpoint of the Leader node
	PeerAddr       []string            // Network endpoints of the other nodes in the Raft cluster
	LastHB         time.Time           // Tracks the last Heartbeat from the Leader node
	PeerC          chan interface{}    // Used for receiving events from peer nodes
	LeaderC        chan interface{}    // Used for channeling events from clients to the Leader
	VotesAck       AckCounter          // Acknowledgement counter for votes
	CommitAck      AckCounter          // Acknowledgement counter for commits on nodes of the cluster
	CommitCycles   UpdateAckCycles     // Count of wait cycles per client during Log Replication
	ClientC        ClientChannel       // Used for channeling events to clients
	Log    		   AppendLog           // Log where client updates are appended
}

func NewRaftNode(addr []string) RaftNode {
	n := RaftNode{}

	n.Type = Follower
	n.GrantedVote = false
	n.CurrentTerm = 0
	n.CurrentIndex = 0
	n.CommittedIndex = 0
	n.Duration = rand.Intn(MaxElectionDuration-MinElectionDuration+1) + MinElectionDuration
	n.VotesCycles = 0
	n.Addr = addr[0]
	n.LeaderAddr = ""
	n.PeerAddr = addr[1:]
	n.LastHB = time.Now()
	n.PeerC = make(chan interface{})
	n.LeaderC = make(chan interface{})
	n.ClientC = ClientChannel{}
	n.VotesAck = AckCounter{}
	n.CommitAck = AckCounter{}
	n.CommitCycles = UpdateAckCycles{}
	n.Log = MemLog{Cache: map[int]string{}}

	return n
}

func (n *RaftNode) IsFollower() bool {
	return n.Type == Follower
}

func (n *RaftNode) IsCandidate() bool {
	return n.Type == Candidate
}

func (n *RaftNode) IsLeader() bool {
	return n.Type == Leader
}

func (n *RaftNode) GetType() string {
	switch n.Type {
		case Follower: return "Follower"
		case Candidate: return "Candidate"
	}
	return "Leader"
}

func (n *RaftNode) isLeaderOk() bool {
	cn := time.Now().UnixNano()
	ln := n.LastHB.UnixNano()
	cms := cn / 1000000
	lms := ln / 1000000

	return (len(n.LeaderAddr) > 0) && ((cms - lms) <= int64(HeartbeatDuration))
}

func (n *RaftNode) ToFollower() {
	n.Type = Follower
}

func (n *RaftNode) ToCandidate() {
	n.Type = Candidate
}

func (n *RaftNode) ToLeader() {
	n.Type = Leader
}

func (n *RaftNode) SetGrantedVote() {
	n.GrantedVote = true
}

func (n *RaftNode) ResetGrantedVote() {
	n.GrantedVote = false
}

func (n *RaftNode) IncrDuration() {
	n.Duration += n.Duration + rand.Intn(DurationIncrRange)
}

func (n *RaftNode) SetLeaderAddr(addr string) {
	n.LeaderAddr = addr
}

func (n *RaftNode) SetCurrentTerm(term int) {
	n.CurrentTerm = term
}

func (n *RaftNode) SetCurrentIndex(index int) {
	n.CurrentIndex = index
}

func (n *RaftNode) SetCommittedIndex(index int) {
	n.CommittedIndex = index
}

func (n *RaftNode) SetLastHB(now time.Time) {
	n.LastHB = now
}

func (n *RaftNode) IncrVoteCycles() {
	n.VotesCycles++
}

func (n *RaftNode) ResetVoteCycles() {
	n.VotesCycles = 0
}

func (n *RaftNode) AllCount() int {
	return len(n.PeerAddr) + 1
}

func (n *RaftNode) QuorumCount() int {
	return ((len(n.PeerAddr) + 1) / 2) + 1
}

/*
   RequestVote message is sent by a Candidate node during Leader Election
 */

type RequestVote struct {
	Term  int
	Index int
	From  string
}

func NewRequestVote(term int, index int, from string) RequestVote {
	r := RequestVote{}

	r.Term = term
	r.Index = index
	r.From = from

	return r
}

/*
   GrantVote message is sent out by Follower node(s) in response to a RequestVote during Leader Election
 */

type GrantVote struct {
	Vote  bool
	Term  int
	Index int
	From  string
}

func NewGrantVote(vote bool, term int, index int, from string) GrantVote {
	g := GrantVote{}

	g.Vote = vote
	g.Term = term
	g.Index = index
	g.From = from

	return g
}

/*
   Heartbeat message is sent out by the Leader node to all the Follower nodes in the cluster
 */

type Heartbeat struct {
	Term  int
	Index int
	From  string
}

func NewHeartbeat(term int, index int, from string) Heartbeat {
	h := Heartbeat{}

	h.Term = term
	h.Index = index
	h.From = from

	return h
}

/*
   AppendEntries message encapsulates an update from a client that needs to be replicated to all the
   nodes in the cluster
 */

type AppendEntries struct {
	Sync   bool
	Term   int
	Index  int
	Leader string // Leader
	Client string // Client
	Entry  string
}

func NewAppendEntries(sync bool, term int, index int, addr string, client string, entry string) AppendEntries {
	a := AppendEntries{}

	a.Sync = sync
	a.Term = term
	a.Index = index
	a.Leader = addr
	a.Client = client
	a.Entry = entry

	return a
}

/*
   CommitAck message is sent by the Follower nodes in response to an AppendEntries message from the Leader
   node after the Follower has appended the update in its Log
 */

type CommitAck struct {
	Ack    bool
	Sync   bool
	Term   int
	Index  int
	Addr   string // Peer
	Client string // Client
}

func NewCommitAck(ack bool, sync bool, term int, index int, addr string, client string) CommitAck {
	c := CommitAck{}

	c.Ack = ack
	c.Sync = sync
	c.Term = term
	c.Index = index
	c.Addr = addr
	c.Client = client

	return c
}

/*
   ClientUpdate message is sent by a client to the Leader node and represents the update from the client
 */

type ClientUpdate struct {
	Addr   string
	Entry  string
	TStamp time.Time
}

func NewClientUpdate(addr string, entry string) ClientUpdate {
	c := ClientUpdate{}

	c.Addr = addr
	c.Entry = entry
	c.TStamp = time.Now()

	return c
}

/*
   UpdateAck message is sent by the Leader node in response to a ClientUpdate message from a client once
   the update has been successfully replicated to a quorum of nodes in the cluster
 */

type UpdateAck struct {
	Ack    bool
	Addr   string // Client
	From   string // Leader
	TStamp time.Time
}

func NewUpdateAck(ack bool, addr string, from string) UpdateAck {
	u := UpdateAck{}

	u.Ack = ack
	u.Addr = addr
	u.From = from
	u.TStamp = time.Now()

	return u
}
