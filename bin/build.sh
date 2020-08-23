rm raft_client raft_server
go build -o raft_server ../raft_server.go ../data.go ../utils.go ../io.go ../rpc.go ../timers.go ../memlog.go ../election.go ../replication.go
go build -o raft_client ../raft_client.go ../data.go ../utils.go ../io.go ../rpc.go ../timers.go ../memlog.go ../election.go ../replication.go

