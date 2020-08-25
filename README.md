### Simple implementation of Raft Consensus algorithm

Sample code that demonstrates the working of Raft Consensus.

### Build Executable(s)

To build the executables `raft_server` and `raft_client`:

```
cd SimpleRaft/bin
./build.sh
```

### Run the cluster on `localhost`

To run a 3-node cluster open 4 command Terminal windows - 3 for the server nodes and one for the client.

In Terminal-1 execute the following:

```
cd SimpleRaft/bin
./n1.sh
```

In Terminal-2 execute the following:

```
cd SimpleRaft/bin
./n2.sh
```

In Terminal-3 execute the following:

```
cd SimpleRaft/bin
./n3.sh
```

Determine which of the 3 server nodes in the cluster has been promoted to the `Leader`.
Assuming it is node in Terminal-1 with IP address `127.0.0.1:9001`, start the client in
Terminal-4 as follows:

```
cd SimpleRaft/bin
./raft_client 127.0.0.1:9001
```

### Article(s)

* [The Raft Consensus Paper](https://raft.github.io/raft.pdf)
* [Understanding Distributed Consensus with Raft](https://medium.com/@kasunindrasiri/understanding-raft-distributed-consensus-242ec1d2f521)
