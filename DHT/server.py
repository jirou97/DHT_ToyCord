#!/usr/bin/env python

import Queue
import random
from client import Client
import socket
import sys, os
import threading
import time
from hashlib import sha1
from threading import Thread
from multiprocessing import Process 
from neighbors import *
from neighbors import send_request



class Server(object):
    """Server class

    Implements all the operations that a DHT server should do"""
    def __init__(self, host, master, defined_port = None):
        """ Protocol of communication between ToyCord DHT Servers. """
        self.operations = {'quit': self._quit,
                           'join': self._join,
                           'next': self._update_my_front,
                           'prev': self._update_my_back,
                           'depart': self._depart,
                           'insert': self._insert,
                           'add': self._add_data, 
                           'add_replica': self._add_replica,
                           'delete': self._delete,
                           'remove': self._remove,
                           'query': self._query,
                           'print_all_data': self._print_all_data,
                           'print_my_data': self._print_my_data,
                           'retrieve': self._retrieve,
                           'retrieve_replicas': self._retrieve_replicas,
                           'bye': self._bye,
                           'overlay':self._overlay,
                           'has_data':self._has_data,
                           'insert_after_depart' : self._insert_after_depart}
        # If true, then the Server stops accepting connections and departs.                           
        self.close = False
        # ID of the node, set by Master after joining the DHT
        self.id = '1'
        # Dictionary of all data. Contains {"hashed_key": (key,value)}
        self.data = {}
        # Dictionary of all replicas. Contains {"hashed_key": (key,value)}
        self.replicas = {}
        # Replication, set by Master after joining the DHT
        self.replication = 0
        # Strategy, set by Master after joining the DHT
        self.strategy = ""
        # Set the master ip and port.
        if master != -1 :
            self.m_host = master.split(":")[0]
            self.m_port = int(master.split(":")[1])
        else :
            self.host = "192.168.1.1"
        # Create a lock to be used while reading or writing data or replicas.
        self.data_lock = threading.Lock()
        self.thread_list = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Reuse the socket port.
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            if master == -1:
            # Create the master on a specific socket
                self.sock.bind(('', 30001))
            else :
                # everyone else on a random one
                if defined_port == None :
                    self.sock.bind(('', 0))
                else :
                    # or the defined port
                    self.sock.bind(('',int(defined_port)))
        except socket.error as msg:
            sys.stderr.write('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()
        # Ip of the server
        self.host = host
        # Port that the Server listens to
        self.port = self.sock.getsockname()[1]
        # hash of the ip:port string
        self.hash = sha1(str(self.host)+":"+str(self.port)).hexdigest()
        self.sock.settimeout(1)
        # Specify number of unaccpeted connections before refusing new connections
        self.sock.listen(10)
        # Has all the information about the Servers neighbor in DHT
        self.neighbors = Neighbors(self.hash, self.port, self.hash, self.port, self.host, self.host)
        # Servers' reply messages
        self.message_queues = {} 

    def __del__(self):
        """Destructor"""
        self.sock.close()

    def DHT_join(self):
        """Servers join the DHT"""
        back_ip, back_port, back_hash,\
        front_ip, front_port, front_hash,\
        self.replication, self.strategy, self.id = \
            find_neighbors(self.hash, self.m_host, self.m_port , self.host, self.port)
        self.replication = int(self.replication)
        # Send a request to find your previous server in the DHT
        self.neighbors.create_back(back_ip, back_hash, back_port, self.host,  self.port, self.hash)
        # Send a request to find your next server in the DHT
        self.neighbors.create_front(front_ip, front_hash, front_port,self.host, self.port, self.hash)
        # Get data from the next server
        self.neighbors.send_front('retrieve:{}:{}:*'.format(self.host,self.port))
        # Get replicas of the data in the k-1 previous servers
        if self.replication > 1 :
            self.neighbors.send_back('retrieve_replicas:{}:{}:{}'.format(self.host, self.port ,self.replication - 1))

    def _overlay(self,data, sock):
        """ After recieving an overlay request
            the server adds his ip:port id=node_id at the end
            of the message and forwards it to his front node"""
        _,start_node, host_list = data.split(':')
        start_node_host ,start_node_port  = start_node.split("/")
        # If this node is the one that started the overlay => cycle
        # So we return the overlay of the DHT
        if start_node_port == str(self.port) and host_list != '->':
            self.message_queues[sock].put(host_list)
        else :
            host_list+= str(self.host) +"/"+str(self.port) + ' id='+ self.id + '-> '
            message = 'overlay:'+start_node +":"+host_list 
            self.message_queues[sock].put(self.neighbors.send_front(message))

    def _retrieve_replicas (self, data , sock):
        """Send data requested to the server
        that will add it as a replica
        data = retrieve_replicas:sender_ip:sender_port, copies """
        command, sender_ip ,sender_port , copies  = data.split(':')
        if int(copies) > 0 :
            #If i didn't start this , I send my data to the starting node
            # and forward the request by reducing copies
            self.data_lock.acquire()
            for key, value in self.data.iteritems():
                req = 'add:{}:{}:1:{}'.format(value[0], value[1], self.hash)
                threading.Thread(target=send_request, args=(sender_ip,sender_port, req)).start()
            self.data_lock.release()
            # if back_node == sender.node return (see replication > network_size) else forward the request reducing copies
            if self.neighbors.back_ip == sender_ip and str(self.neighbors.back_port) == sender_port :
                self.message_queues[sock].put('Done')
            else :
                self.neighbors.send_back('retrieve_replicas:' + sender_ip + ':' + sender_port + ':' + str( int(copies) - 1) )
        self.message_queues[sock].put('Done')

    def _retrieve(self, data, sock):
        """Send data requested to previous server
        If next server doesn't have data, then remove
        data = retrieve:key"""
        _, sender_ip, sender_port, find_key = data.split(':')
        # Wildcard
        if find_key == '*':
            res = []
            self.data_lock.acquire()
            for key, value in self.data.iteritems():
                if not self.belongs_here(key):
                    #deletes the replicas
                    self.neighbors.send_front('retrieve:{}:{}:{}'.format(sender_ip,sender_port,key))
                    #if self.neighbors.send_front('retrieve:{}:{}:{}'.format(sender_ip,sender_port,key)) == 'None:None':
                    res.append(key)
                    #insert:key:value
                    req = 'insert:{}:{}'.format(value[0],value[1])
                    threading.Thread(target=send_request, args=(sender_ip,sender_port,req)).start()         
            for key in res:
                del self.data[key]

            res2 = []
            for key, value in self.replicas.iteritems():
                #if i have the last_replica I delete it
                if self.last_replica(key):
                    res2.append(key) 
                else :
                #else find the last replica and delete it
                    self.neighbors.send_front('retrieve:{}:{}:{}'.format(sender_ip,sender_port,key))
            for key in res2 :
               del self.replicas[key]

            self.message_queues[sock].put('Done')
            self.data_lock.release()
        # Single retrieval
        else:
            #deletes last replica...
            self.data_lock.acquire()
            key, value = self.replicas.pop(find_key, (None, None))
            if key is not None:
                self.neighbors.send_front(data)
                #if self.neighbors.send_front(data) == 'None:None':
                #    del self.replicas[find_key]
            self.data_lock.release()
            self.message_queues[sock].put('{}:{}'.format(key, value))

    def last_replica(self,key):
        if self.neighbors.send_front('has_data:{}'.format(key)) == 'None:None':
            return True
        return False

    def _has_data(self,data,sock):
        hashed_key = data.split(':')[1]
        k,v = self.replicas.get(hashed_key,(None,None))
        self.message_queues[sock].put('{}:{}'.format(k,v))

    def _update_my_front(self, data, sock):
        """Updates front neighbor
        data = next:ip:port:hash"""
        _, front_ip, front_port, front_hash = data.split(':')
        self.neighbors.update_front(front_ip, front_hash, int(front_port))
        self.message_queues[sock].put(self.host + ': Connection granted...')

    def _update_my_back(self, data, sock):
        """Updates back neighbor
        data = prev:ip:port:hash"""
        _, back_ip, back_port, back_hash = data.split(':')
        self.neighbors.update_back(back_ip, back_hash, int(back_port))
        self.message_queues[sock].put(self.host + ': Connection granted...')

    def belongs_here(self, key):
        """Decides whether a certain hash belongs in this server"""
        return (self.neighbors.back_hash < key <= self.hash) or \
               (key <= self.hash <= self.neighbors.back_hash) or \
               (self.hash <= self.neighbors.back_hash <= key)

    def _join(self, data, sock):
        """Command he receives to determine where a new server belongs
        data = join:hash"""
        _, key_hash, _, _ = data.split(':')
        if self.belongs_here(key_hash):
            message = self.neighbors.get_back() + ':' + str(self.host) + ':' +str(self.port) + ':' + self.hash
        else:
            message = self.neighbors.send_front(data)
        self.message_queues[sock].put(message)

    def _depart(self, data, sock, forward=True):
        """Function to gracefully depart from DHT
        If forward=False, then the DHT is shutting down
        and we don't need to move the data.
        data = depart:node_id"""
        if forward:
            self.send_replicas_forward()
            time.sleep(1)
            self.send_data_forward()
        # Let the previous node know who its new next node is, after I depart
        self.neighbors.send_back('next:{}:{}:{}'.format(self.neighbors.front_ip,self.neighbors.front_port, self.neighbors.front_hash))
        # Let the next node know who its new previous node is, after I depart
        self.neighbors.send_front('prev:{}:{}:{}'.format(self.neighbors.back_ip,self.neighbors.back_port, self.neighbors.back_hash))
        # forward = False means all have to exit and not pass their values
        
        # Else let the master know that you departed
        if forward == True :
            send_request(self.m_host, self.m_port, 'depart:'+self.id)
        self.close = True
        self.message_queues[sock].put('Done...Bye Bye')

    def send_data_forward(self):
        """In case of departing, sends all stored data to the next server"""
        front_ip = self.neighbors.front_ip
        front_port = self.neighbors.front_port
        self.data_lock.acquire()
        for key, value in self.data.iteritems():
            #Process(target= lambda : self.neighbors.send_front('insert_after_depart:{}:{}'.format(value[0],value[1]))).start()
            req = 'insert_after_depart:{}:{}'.format(value[0],value[1])
            threading.Thread(target=send_request , args = (front_ip, front_port, req)).start()
            #threading.Thread(target=send_request , args = (front_ip, front_port, req)).start()
        self.data_lock.release()

    def send_replicas_forward(self):
        """In case of departing, sends all stored replicas to the next server"""
        self.data_lock.acquire()
        for key, value in self.replicas.itervalues():
            Process(target= lambda : self.neighbors.send_front('add:{}:{}:1:{}'.format(key, value, self.hash))).start()
        self.data_lock.release()
    
    def _insert_after_depart(self, data, sock):
        """A new (key, value) pair is inserted
        If it doesn't belong to us, send it forward
        Otherwise add replication-1
        data = insert:key:value"""
        _, key, value = data.split(':')
        key_hash = sha1(key).hexdigest()
        self.data_lock.acquire()
        
        if self.belongs_here(key_hash):
            # If it belongs here we add it or update it
            self.data[key_hash] = (key, value)
            self.replicas.pop(key_hash, None)
            self.data_lock.release()
            #eventual : We should instantly inform that we are done.
            if self.strategy == "eventual" or self.replication == 1 :
               sock.send("Eventual_Done")
               self.message_queues[sock].put("Eventual_Done")
            if self.replication > 2 :
                if self.strategy == 'linear' :
                    # Make sure everyone added the new pair and then return
                    while True :
                        if self.neighbors.send_front('add:{}:{}:{}:{}'.format(key, value, str(self.replication - 2), self.hash)) != None :
                            break
                    self.message_queues[sock].put(value)
                elif self.strategy == 'eventual' :
                    # create a new process that adds the replica to the next node
                    self.message_queues[sock].put("Eventual_Done")
                    Process(target = lambda : self.neighbors.send_front('add:{}:{}:{}:{}'.format(key, value, str(self.replication - 2), self.hash))).start()
            elif self.replication == 2 :
                if self.strategy == 'linear' :
                # Make sure everyone added the new pair and then return
                    while True :
                        if self.neighbors.send_front('add:{}:{}:{}:{}'.format(key, value, str(self.replication - 1), self.hash)) != None :
                            break
                    self.message_queues[sock].put(value)
                elif self.strategy == 'eventual' :
                    # create a new process that adds the replica to the next node
                    self.message_queues[sock].put("Eventual_Done")
                    Process(target = lambda : self.neighbors.send_front('add:{}:{}:{}:{}'.format(key, value, str(self.replication - 1), self.hash))).start()

        else:
            #if it doesn't belong here then forward it
            self.data_lock.release()
            self.message_queues[sock].put(self.neighbors.send_front(data))

    def _add_replica(self, data, sock):
        """Someone in the back of us wants to add
        more replicas of what they sent me
        If I already have it, then push it forward
        data = add_replica:key:value:copies:host"""
        _, key, value, copies, host = data.split(':')
        key_hash = sha1(key).hexdigest()
        self.data_lock.acquire()
        #If I don't have it add it as a replica
        if self.replicas.get(key_hash, None) != (key, value):
            self.replicas[key_hash] = (key, value)
            self.message_queues[sock].put(value)
        else : 
        #else I already have it => tell the front node to add it as a replica
            self.message_queues[sock].put('Done')
                    #self.neighbors.send_front('add_replica:{}:{}:{}:{}'.format(key, value, copies, host)))
           
        self.data_lock.release()

    def _bye(self, data, sock):
        """DHT is shutting down
        data = bye"""
        self._depart(data, sock, forward=False)
        t = threading.Thread(target=send_request, args=(self.neighbors.front_ip, self.neighbors.front_port, 'bye'))
        t.start()

    def _add_data(self, data, sock):
        """Someone in the back of us wants to add
        more replicas of what they sent me
        If I already have it, then push it forward
        data = add:key:value:copies:host"""
        _, key, value, copies, host = data.split(':')
        # No more replicas to add or circle
        if (copies == '0') or (host == self.hash):
            self.message_queues[sock].put(value)
        else:
            key_hash = sha1(key).hexdigest()
            self.data_lock.acquire()
            if copies == '-2' :
                self.data[key_hash] = (key, value)
                del self.replicas[key_hash]
            elif self.replication > 1  :
            #add it as replica
                # If dont have it , add it as a replica and reduce copies
                if self.replicas.get(key_hash, None) != (key, value) :
                    self.replicas[key_hash] = (key, value)
                    copies = str(int(copies) - 1)
            else :
            #add it as data
                if self.data.get(key_hash, None) != (key, value) :
                    self.data[key_hash] = (key, value)
                    copies = str(int(copies) - 1)
            self.data_lock.release()

            if copies == '0': # If copies = 0 then I have the last copy
                self.message_queues[sock].put(value)
            elif copies > '0':
                if self.strategy == 'linear':
                    # We have to make sure that the replica is added before we return
                    while True :
                        if self.neighbors.send_front('add:{}:{}:{}:{}'.format(key, value, copies, host)) != None :
                            break
                    self.message_queues[sock].put(value)
                elif self.strategy == 'eventual' :
                    # We can return we are added the replica, after informing our front node
                    #Process(target = lambda : self.neighbors.send_front('add:{}:{}:{}:{}'.format(key, value, copies, host))).start()
                    #self.message_queues[sock].put("Eventual_Done")
                    self.neighbors.send_front('add:{}:{}:{}:{}'.format(key, value, copies, host))
                    
                    self.message_queues[sock].put("Eventual_Done")
            else :
                self.message_queues[sock].put('Done')       

    def _insert(self, data, sock):
        """A new (key, value) pair is inserted
        If it doesn't belong to us, send it forward
        Otherwise add replication-1
        data = insert:key:value"""
        _, key, value = data.split(':')
        key_hash = sha1(key).hexdigest()
        self.data_lock.acquire()
        
        if self.data.get(key_hash, (None, None))[1] == value:
            # If I already have with the same value return
            self.data_lock.release()
            self.message_queues[sock].put(value)
        elif self.belongs_here(key_hash):
            # If it belongs here we add it or update it
            self.data[key_hash] = (key, value)
            self.data_lock.release()
            #eventual : We should instantly inform that we are done.
            if self.strategy == "eventual" or self.replication == 1 :
               sock.send("Eventual_Done")
               self.message_queues[sock].put("Eventual_Done")
            if self.replication > 1 :
                if self.strategy == 'linear' :
                    # Make sure everyone added the new pair and then return
                    while True :
                        if self.neighbors.send_front('add:{}:{}:{}:{}'.format(key, value, str(self.replication - 1), self.hash)) != None :
                            break
                    self.message_queues[sock].put(value)
                elif self.strategy == 'eventual' :
                    # create a new process that adds the replica to the next node
                    self.message_queues[sock].put("Eventual_Done")
                    Process(target = lambda : self.neighbors.send_front('add:{}:{}:{}:{}'.format(key, value, str(self.replication - 1), self.hash))).start()
                                     
        else:
            #if it doesn't belong here then forward it
            self.data_lock.release()
            self.message_queues[sock].put(self.neighbors.send_front(data))

    def _delete(self, data, sock):
        """Deletes key, value
        Same as insert
        data = delete:key"""
        _, key = data.split(':')
        key_hash = sha1(key).hexdigest()
        #if its in the data delete it and tell front node to do the same.
        if self.belongs_here(key_hash):
            self.data_lock.acquire()
            answer = self.data.pop(key_hash, (None, None))
            self.data_lock.release()
            # if it exists we should also delete the replicas.
            if answer[0] is not None:
                if self.strategy == 'eventual':
                    Process(target = lambda : self.neighbors.send_front('remove:{}'.format(key))).start()
                    sock.send('Eventual_Done')
                elif self.strategy == 'linear' :
                    while True :
                        if self.neighbors.send_front('remove:{}'.format(key)) != None :
                            break
                self.message_queues[sock].put('{}:{}'.format(*answer))
        else:
            self.neighbors.send_front(data)
            self.message_queues[sock].put('Done')

    def _remove(self, data, sock):
        """Removes key, same as add
        data = remove:key"""
        _, key = data.split(':')
        key_hash = sha1(key).hexdigest()
        self.data_lock.acquire()
        answer = self.replicas.pop(key_hash, (None, None))
        self.data_lock.release()
        if answer[0] is not None:
            # if it exists we should inform our front node.
            if self.strategy == 'eventual':
                sock.send('Eventual_Done')
                self.neighbors.send_front('remove:{}'.format(key))

            elif self.strategy == 'linear' :
                while True :
                    if self.neighbors.send_front('remove:{}'.format(key)) != None :
                        break
            self.message_queues[sock].put('{}:{}'.format(*answer))
        self.message_queues[sock].put('Done')

    def _query(self, data, sock):
        """Searches for a key
        data = 'query:copies:hostkey:key"""
        _, copies, host, song = data.split(':')
        key = sha1(song).hexdigest()
        # Find if you have it as a data or replica
        self.data_lock.acquire()
        value1 = self.data.get(key, None)
        value2 = self.replicas.get(key, None)
        self.data_lock.release()
        # Always the node with the last replica replies
        if self.strategy == 'linear':
            # Message has passed through the server it belongs
            if copies != '-1':
                # Find the last replica
                if int(copies) > 1:
                    copies = str(int(copies) - 1)
                    self.message_queues[sock].put(self.neighbors.send_front('query:{}:{}:{}'.format(copies, self.host, song)))
                # Last replica
                else:
                    if value2 == None:
                        value2 = "'{}' does not exist as a replica.".format(song)
                    if value1 != None :
                        value2 = "{}".format(value1)
                    self.message_queues[sock].put("Answer from last chain's node {}:{} :{}".format(self.host,self.port,value2))
            elif self.belongs_here(key):
                # Find the last replica
                if self.replication > 1:
                    copies = str(self.replication - 1)
                    self.message_queues[sock].put(self.neighbors.send_front('query:{}:{}:{}'.format(copies, self.host, song)))
                # Only replica
                else:
                    # Onlye replica
                    if value1 == None and value2 != None:
                        self.message_queues[sock].put('Only replica found in {}:{} :{}'.format(self.host,self.port, value2))
                    #Only data
                    elif value1 != None :
                        self.message_queues[sock].put('Only data found in {}:{} :{}'.format(self.host,self.port, value1))
                    #Nor a data or a replica
                    else :
                        self.message_queues[sock].put("'{}' does not exist. Answer from {}:{} ".format(song, self.host,self.port))
            else:
                #else forward it
                self.message_queues[sock].put( 
                    self.neighbors.send_front('query:{}:{}:{}'.format(copies, host, song)))
        else : # Eventual : Any node that has the data can reply
            #I have it as data
            if value1 != None  :
                self.message_queues[sock].put('Answer from {}:{} :{}'.format(self.host,str(self.port), value1))
            #I have it as a replica
            elif value2 != None  :
                self.message_queues[sock].put('Answer (Replica) from {}:{} :{}'.format(self.host,str(self.port), value2))

            #if it belongs to me and I dont have it , it doesn't exist
            elif self.belongs_here(key) and value1 == None:
                self.message_queues[sock].put('Answer from {}:{} :{} does not exist'.format(self.host,str(self.port), song))

            else : #forward it
                self.message_queues[sock].put(
                    self.neighbors.send_front('query:{}:{}:{}'.format(copies, host, song)))

    
    def _print_my_data(self, data, sock):
        """Prints my data and forwards the message
        data = print_my_data:hash:data_of_nodes"""
        _, sender_ip , sender_port, sender_hash = data.split(':')
        if self.hash != sender_hash:
            self.data_lock.acquire()
            if self.replication  > 1:
                x = '\n' +self.host +"/"+str(self.port) + " with id "+ self.id +'\n Data->' + str([value for value in self.data.itervalues()]) + '\n' +' Replicas->' + str([value for value in self.replicas.itervalues()]) 
            else :
                x = '\n' +self.host +"/"+str(self.port) + " with id "+ self.id +'\n Data->' + str([value for value in self.data.itervalues()]) 
            self.data_lock.release()
            # Send your data to the master
            send_request(sender_ip,int(sender_port),'master_print_my_data:' + x)
            # And commands the next node to do the same.
            self.message_queues[sock].put(self.neighbors.send_front(data))
        else:
            self.message_queues[sock].put(" ")

    def _print_all_data(self, data, sock):
        """Starts data printing of all servers
        data = print_all_data"""
        self.data_lock.acquire()
        if self.replication > 1:
            x = self.host + "/" + str(self.port) + " with id " + self.id + '\n Data->' + str([value for value in self.data.itervalues()]) + '\n' +' Replicas->' + str([value for value in self.replicas.itervalues()])
        else :
            x = self.host + "/" + str(self.port) + " with id " + self.id + '\n Data->' + str([value for value in self.data.itervalues()]) + '\n'
        #prints masters data and then commands next server to 
        #print its data.
        print x
        self.data_lock.release()
        if self.neighbors.front_hash != self.hash:
            self.message_queues[sock].put(self.neighbors.send_front('print_my_data:' + self.host + ':' + str(self.port) + ':' + self.hash))
        else:
            self.message_queues[sock].put(x)

    def _quit(self, data, sock):
        """Quits"""
        self.message_queues[sock].put('CLOSE CONNECTION')

    def _reply(self, data, sock):
        """Bad Request"""
        self.message_queues[sock].put('Server does not support this operation')

    def _connection(self):
        """Main function to serve a connection"""
        try:
            conn, _ = self.sock.accept()
        except socket.timeout:
            pass
        else:
            self.message_queues[conn] = Queue.Queue()
            threading.Thread(target=self.clientthread, args=(conn,)).start()

    def clientthread(self, sock):
        """A thread executes the command"""
        while True:
            try:
                data = sock.recv(16384)
                if not data:
                    break
                else:
                    if data == "print_all_data" :
                        # Start printing all data (query,*)
                        self._print_all_data(data,sock)
                        data = self.message_queues[sock].get_nowait()
                        self.message_queues[sock].put(data)
                    elif data.startswith('master_print_my_data'):
                        print data.split(':')[1]
                        self.message_queues[sock].put(data.split(':')[-1])
                    elif data.startswith("choose_random:"):
                        # Executes the command starting from a random server
                        fun = self.operations.get(data.split(':')[1], self._reply)
                        fun(data, sock)
                    else :
                    #    Executes the command starting from a specific server
                        fun = self.operations.get(data.split(':')[0], self._reply)
                        fun(data, sock)
            except socket.error:
                sys.stderr.write('Data recv error')
                break
            else:
                try:
                    # get the last message of the queue and return it
                    new_msg = self.message_queues[sock].get_nowait()
                except Queue.Empty:
                    pass
                else:
                    sock.send(new_msg)
                    #
                    if new_msg == 'CLOSE CONNECTION':
                        del self.message_queues[sock]
                        sock.close()
                        return

    def accept_connection(self):
        """Main loop"""
        while True:
            self._connection()
            if self.close:
                time.sleep(2)
                return

    def get_port(self):
        """Returns port"""
        return self.port

    def get_sock(self):
        """Returns socket"""
        return self.sock

class Server_master(Server):
    """First server of the DHT"""
    def __init__(self, host, repl, strat):
        # Has the current network size
        self._network_size = 1
        # Has the number of nodes joined
        self._max_network_size = 1
        self.close = False
        super(Server_master, self).__init__(host, -1, 30001)
        self.replication = int(repl)
        self.strategy = strat
        # dictionary that has all the IPs and Ports of all servers that has joined the DHT
        self.ports =  {}
        self.ports['1'] = [host, self.get_port()]

    def _join(self, data, sock):
        """
            A server joins the DHT
        """
        self._network_size += 1
        self._max_network_size += 1
        splited = data.split(":")
        self.ports[str(self._max_network_size)] = [splited[2], splited[3]]
        super(Server_master, self)._join(data, sock)
        self.message_queues[sock].put(self.message_queues[sock].get() + ':' + str(self.replication) + ':' + self.strategy + ':' + str(self._max_network_size))

    def _depart(self, data, sock, forward=False):
        """
            A server departs from the DHT
        """
        server_id = data.split(":")[-1]
        host =self.ports.pop(server_id,None)
        if host != None :
            ip = host[0]
            port = host[1]
            # Force server to depart
            with Client(ip,port) as cli:
                cli.communication("depart")
            self._network_size -= 1
        self.message_queues[sock].put('Done')

    def _insert(self, data, sock):
        """
            Inserts data in DHT
        """
        # Start from specified node 
        if "start_from" in data:
            _, _, _, server_id, key, value = data.split(":")
            data = 'insert:'+ key +':' + value
            host , port = self.ports[server_id]
            # If the chosen node is the master, we have to execute
            # the insert function of the Class it extends (Server)
            if host == self.host and port == self.port:
                    super(Server_master, self)._insert(data, sock)
            else :
                # Send the message to the specified server
                with Client(host, port) as cli:
                    self.message_queues[sock].put(cli.communication(data))
        else :
            # Start from random node 
            if data.startswith("choose_random"):
                data = data.replace("choose_random:","")
                serv_id  = random.sample(self.ports, 1)[0]
                host , port = self.ports[serv_id]
                # If the random node is the master, we have to execute
                # the insert function of the Class it extends (Server)
                if host == self.host and port == self.port:
                    super(Server_master, self)._insert(data, sock)
                else :
                    # Send the message to the chosen server
                    with Client(host, port) as cli:
                        self.message_queues[sock].put(cli.communication(data))
            else :
                # Master recieves a insert:key:value from another node
                super(Server_master, self)._insert(data, sock)
        
    def _delete(self, data, sock):
        """
            Deletes data from DHT
        """
        # Start from specific node 
        if "start_from" in data:
            _, _, _, server_id, key= data.split(":")
            data = 'delete:'+ key 
            host , port = self.ports[server_id]
            # If the chosen node is the master, we have to execute
            # the delete function of the Class it extends (Server)
            if host == self.host and port == self.port:
                    super(Server_master, self)._delete(data, sock)
            else :
                # Send the message to the specified server
                with Client(host, port) as cli:
                    self.message_queues[sock].put(cli.communication(data))
        else :
            # Start from random node 
            if data.startswith("choose_random"):
                data = data.replace("choose_random:","")
                serv_id  = random.sample(self.ports, 1)[0]
                host , port = self.ports[serv_id]
                # If the random node is the master, we have to execute
                # the delete function of the Class it extends (Server)
                if host == self.host and port == self.port:
                    super(Server_master, self)._delete(data, sock)
                else :
                    # Master recieves a delete:key from another node
                    with Client(host, port) as cli:
                        self.message_queues[sock].put(cli.communication(data))
            else :
                super(Server_master, self)._delete(data, sock)

    def _query(self, data, sock):
        """
            Queries data in DHT
        """
        # Start from specific node 
        if "start_from" in data:
            _, _, _, server_id, v1, v2, key = data.split(":")
            data = 'query:'+ v1 + ':' + v2 + ':' + key 
            host , port = self.ports[server_id]
            # If the chosen node is the master, we have to execute
            # the insert function of the Class it extends (Server)
            if host == self.host and port == self.port:
                super(Server_master, self)._query(data, sock)
            else :
                # Send the message to the specified server
                with Client(host, port) as cli:
                    self.message_queues[sock].put(cli.communication(data))
        else :
            # Start from random node 
            if data.startswith("choose_random"):
                data = data.replace("choose_random:","")
                serv_id  = random.sample(self.ports, 1)[0]
                host , port = self.ports[serv_id]
                # If the random node is the master, we have to execute
                # the query function of the Class it extends (Server)
                if host == self.host and port == self.port:
                    super(Server_master, self)._query(data, sock)
                else :
                    # Send the message to the chosen server
                    with Client(host, port) as cli:
                        self.message_queues[sock].put(cli.communication(data))
            else :
                # Master recieves a query:key not initiated from another node
                super(Server_master, self)._query(data, sock)

    def _bye(self, data, sock):
        # Destroys the DHT
        if self._network_size > 1:
            thr = threading.Thread(target=send_request, args=(self.neighbors.front_ip,self.neighbors.front_port, 'bye'))
            thr.start()
            self._network_size = 1
        else:
            self.close = True
        self.message_queues[sock].put('Done')

#Function that returns the ip of the current node.
def get_ip():
    text = os.popen('ip addr show eth1').read().split("inet ")[1].split("/")[0]
    result = text.startswith("192.") 
    if result :
        return os.popen('ip addr show eth1').read().split("inet ")[1].split("/")[0]
    else :
        return os.popen('ip addr show eth2').read().split("inet ")[1].split("/")[0]

if __name__ == "__main__": 
    try :
        port = sys.argv[1]
    except :
        port = None
    server = Server(get_ip(),"192.168.1.1:30001",port) #Creates a new server and sets the Master Server
    server.DHT_join()   # Newly created node joins the dht
    server.accept_connection() # And starts accepting connections