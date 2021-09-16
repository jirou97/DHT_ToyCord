from client import Client

import time
import sys

''' Class tha implements the neighbors of a server in DHT '''

class Neighbors(object):

    def __init__(self, back_hash, back_port, front_hash, front_port, back_ip, front_ip):
        self.back_hash = back_hash
        self.front_hash = front_hash
        self.back_port = int(back_port)
        self.front_port = int(front_port)
        self.back_ip = back_ip
        self.front_ip = front_ip
    
    def create_back(self, new_back_ip, new_back_hash, new_back_port, host1, port1, myhash):
        with Client(new_back_ip,int(new_back_port)) as cli:
            cli.communication('next:' +str(host1)+":" +str(port1)+ ':' + myhash)
        self.back_hash = new_back_hash
        self.back_port = int(new_back_port)
        self.back_ip = new_back_ip
        
    def create_front(self,new_front_ip, new_front_hash, new_front_port,host1, port1, myhash):
        with Client(new_front_ip,int(new_front_port)) as cli:
            cli.communication('prev:' +str(host1)+":" +str(port1)+ ':' + myhash)
        self.front_hash = new_front_hash
        self.front_port = int(new_front_port)
        self.front_ip = new_front_ip

    def update_back(self,new_back_ip, new_back_hash, new_back_port):
        self.back_ip = new_back_ip
        self.back_hash = new_back_hash
        self.back_port = int(new_back_port)
        
    def update_front(self, new_front_ip, new_front_hash, new_front_port):
        self.front_ip = new_front_ip
        self.front_hash = new_front_hash
        self.front_port = int(new_front_port)

    def send_back(self,data):
        with Client(self.back_ip, int(self.back_port)) as cli:
            return cli.communication(data)

    def send_front(self,data):
        with Client(self.front_ip, int(self.front_port)) as cli:
            return cli.communication(data)

    def get_front(self):
        return str(self.front_ip) + ':' + str(self.front_port) + ':' + self.front_hash

    def get_back(self):
        return str(self.back_ip) + ':' +str(self.back_port) + ':' + self.back_hash


def find_neighbors(hash_value,IP, PORT ,myIP, myPORT):
    with Client(IP, PORT) as cli:
        x = cli.communication('join:' + hash_value+":"+myIP+":"+str(myPORT)).split(':')   
    #x = back_ip ,back_port, back_hash , new.ip , new.port , new.hash , repl , strategy, node_id
    x[1] = int(x[1])
    x[4] = int(x[4])
    return x
                                
def send_request(IP, PORT,message):
    with Client(IP,int(PORT)) as cli:
        return cli.communication(message)

    
