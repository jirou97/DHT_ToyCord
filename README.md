# DHT - ToyCord, a simplified version of DHT Chord

Project created by me and [Panagiotis](https://github.com/souliotispanagiotis)


# :toolbox: Tools
- Python 2.7
- socket library
- multiprocessing and thread parallelization tools

#  How to use
## Setup the master node
python main.py {replication_size} {consistency_strategy}

consistency_strategy can be linear or eventual

master is at 192.168.1.1:30001

## Add node to the DHT
start_servers.sh new_node_ip new_node_port

This requires the node to be on the same network.

To change that the master node needs to have a public ip and on server.py you have to replace "192.168.1.1:30001" with the masters ip

You might have to change the get_ip() function on the server.py file, to get the public ip over the private.

### To see the available commands type: help
