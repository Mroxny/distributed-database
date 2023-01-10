# distributed-database
Small TCP distributed database project.

Description
----
This project is a simple implementation of a distributed database that consists of a set of processes located in a network. Each process stores a certain set of information (for simplicity we assume that it's just one pair <key,value> per process).

The network is incrementally created by executing consecutive processes (creation of consecutive nodes) and connecting them to the already existing network. After starting, each process (except for the first one) connects to the network and starts waiting for possible connections from new system components (next added nodes) or from clients willing to reserve the resources.

The communication between the nodes during the database operation uses TCP.

Installation
----

1. Make sure you have [Java](https://www.java.com/en/download/) installed on your machine.
2. Clone or download the repository to your local machine.
3. Compile the code using `javac` command.

Usage
----

**To create a new node:** </br>

```
java DatabaseNode -tcpport <port> -record <key>:<value> [-connect <address>:<port>]
```

where:

* `tcpport` denotes the number of the TCP port, which is used to wait for connections from clients.
* `record` denotes a pair of integers being the initial values stored in this node, where the first number is the key and the second is the value associated with this key.
* `[ -connect ]` denotes a list of other nodes already in the network, to which this node should connect and with which it may communicate to perform operations. This list is empty for the first node in the network.

**To create a new client:** </br>

```
java DatabaseClient -gateway <address>:<port> -operation <operation with parameters>
```

where:

* `gateway` denotes the IP address and the TCP port number of any node already connected to the network (called later the “contact node”)
* `operation` denotes the desired operation with it's parameters

**Available operations:** </br>

* `set-value <key>:<value>`  set a new value (the second parameter) for the key being the first parameter. The result of this operation is either an OK message if operation succeeded or ERROR if the database contains no pair with a requested key value. If the database contains more than 1 such pair, at least one must be altered.

* `get-value <key> ` get a value associated with the key being the parameter. The result of this operation is a message consisting of the value or an error message

* `find-key <key>` find the address and the port number of a node, which hosts a pair with the key value given as the parameter. If such node exists, the answer is a pair <address>:<port> identifying this node, or the message ERROR if no node has a key with such a value. If the database contains more than 1 such node, only one pair must returned (any valid).

* `get-max` return a max key and value from database, returns error if DB is empty

* `get-min` return a min key and value from database, returns error if DB is empty

* `get-cons` return a list of currently avaliable connections with this node

* `new-record <key>:<value>` remember a new pair key:value instead of the pair currently stored in the node to which the client is connected. The result of this operation is the OK message.

* `terminate` detaches the node from the database. The node must inform its neighbours about this fact and terminate. The informed neighbours store this fact in their resources ans no longer communicate with it. Just before the node terminates, it sends back the OK message to a client.

Examples
----

**Creating a new node:** </br>

```
java DatabaseNode -tcpport 9991 -record 17:256 -connect localhost:9990 -connect localhost:9997 -connect localhost:9989
```

This command means execution of a node which holds a value 256 associated with a key 17, listens to the TCP port 9991 for connections from clients and other nodes willing to connect and to connect itself to the network it should use the nodes working on a computer with an address localhost and TCP ports 9990, 9997 and 9989.

**Creating a new client:** </br>

```
java DatabaseClient -gateway localhost:9991 -operation get-value 17
```

This command means execution of a client, which will use the node working on the machine with address localhost and TCP port number 999
