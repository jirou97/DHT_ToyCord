"""Contains the DHT class which builds a DHT
and provides all the necessary functions to interact
with the DHT"""

import sys
import random
import time

from multiprocessing import Process, Queue
from client import Client
from server import Server, Server_master
from collections import OrderedDict

class DHT(object):
    """DHT class"""

    def __init__(self, replication,strategy):
        """Constructor"""
        self.help_dict = OrderedDict([
                        ('Node with ID departs from the DHT               ', 'depart,ID                    '),
                        ('Insert key,value pair in DHT                    ', 'insert,key,value             '),
                        ('Delete key from DHT                             ', 'delete,key                   '),
                        ('Query key,value pair in DHT                     ', 'query,key                    '),
                        ('Insert key,value in DHT starting from nodeID    ', 'insert_from,Node_ID,key,value'),
                        ('Delete key from DHT starting from nodeID        ', 'delete_from,Node_ID,key      '),
                        ('Query key,value pair in DHT starting from nodeID', 'query_from ,Node_ID,key      '),
                        ('Inserts file insert.txt in DHT                  ', 'insertfile,insert.txt        '),
                        ('Queries file query.txt in DHT                   ', 'queryfile,query.txt          '),
                        ('Parses file requests.txt in DHT                 ', 'parsefile,requests.txt       '),
                        ('Prints commands of DHT cli                      ', 'help                         '),
                        ('Prints the DHTs overlay                         ', 'overlay                      '),
                        ('Destroys DHT                                    ', 'exit                         ')])
        self.command_dict = {   
                                'depart': self.depart,
                                'exit': self.DHT_destroy,
                                'insert': self.insert,
                                'delete': self.delete,
                                'query': self.query,
                                'insert_from': self.insert_from_node,
                                'delete_from': self.delete_from_node,
                                'query_from': self.query_from_node,
                                'help' : self.print_help,
                                'insertfile': self.insertfile,
                                'queryfile': self.queryfile,
                                'parsefile': self.parsefile,
                                'overlay': self.overlay,
                                'cli': self.cli
                            }
        self.processes = {}
        #contains the host,port of the main server.
        self.ports = {} 
        self.queue = Queue()
        #creates the DHT 
        self.processes['1'] = Process(target=self.create_DHT,
                                      args=(replication,strategy))
        self.processes['1'].start()
        self.host, self.main_port = self.queue.get()
        self.ports['1'] = [self.host, self.main_port]
        time.sleep(1)

    def cli(self):
        """Basic ToyCord CLI that executes the user input"""
        command =''
        while command != 'exit' :
            command = str(raw_input("Write a command or help : "))
            #['join', str(i)]
            splited = command.split(',')
            try :
                self.execute(splited)
            except Exception as e :
                if command != 'exit':
                    print "-->Error trying to execute command:", e.message
                    print "-->Try again or type help!"
                pass

    def __del__(self):
        for proc in self.processes:
            proc.join()

    def execute(self, command):
        """Executes a single command given by the user."""
        fun = self.command_dict.get(command[0], self.bad_command)
        return fun(command)

    @staticmethod
    def bad_command(command):
        """Informs the user that he has enterred an invalid command"""
        sys.stderr.write('Invalid command, type help to see the full list of commands\n')

    def depart(self, command):
        """Commands the specified server to shut down"""
        host, port = self.ports['1']
        with Client(host, port) as cli:
            cli.communication(command[0]+':'+str(command[1]))

    def DHT_destroy(self, command):
        """Forces the whole DHT to shutdown by sending bye to the Master server"""
        host, port = self.ports['1']
        with Client(host, port) as cli:
            cli.communication('bye')

    def insertfile(self,command):
        filename = command[1]
        self.measure_throughput(filename,'Write')

    def queryfile(self,command):
        filename = command[1]
        self.measure_throughput(filename,'Read')

    def parsefile(self,command):
        filename = command[1]
        self.measure_throughput(filename,'Request')

    def measure_throughput(dht, filename, wrr):
        """Reads requests from filename and forwards
        each of them to a random server"""
        start = time.time()
        request_count = 0
        with open(filename, 'r') as f:
            for file_line in f:
                line = file_line.rstrip()
                if line.startswith('query'):
                    dht.execute(line.split(', '))
                elif len(line.split(', ')) == 1:
                    dht.execute(['query', line])
                elif line.startswith('delete'):
                    dht.execute(line.split(', '))                
                elif line.startswith('insert'):
                    dht.execute(line.split(', '))
                elif len(line.split(', ')) == 2:
                    dht.execute(['insert'] + line.split(', '))
                else:
                    raise ValueError('Bad Request %s' % line)
                request_count += 1
        print wrr + ' throughput:' + str(1000* (time.time() - start) / request_count)+' msec/key' 
    
    def overlay(self,command):
        """Sends a request to the master server to print the DHT nodes' overlay"""
        host, port = self.host , self.main_port
        with Client(host, port) as cli:
            overlay_list = cli.communication('overlay:{}/{}:->'.format(host,port))
            print overlay_list

    def insert(self, command):
        """Sends a request to a random server to insert a (key, value) pair"""
        host, port = self.ports['1']
        with Client(host, port) as cli:
            cli.communication('choose_random:{}:{}:{}'.format(*command))

    def insert_from_node(self, command):
        """Sends a request to a specified server to insert a (key, value) pair"""
        host, port = self.ports['1']
        with Client(host, port) as cli:
            cli.communication('insert:start_from:{}:{}:{}:{}'.format(*command))

    def delete(self, command):
        """Sends a request to a random server to delete the key from the DHT"""
        host, port = self.ports['1']
        with Client(host, port) as cli:
            cli.communication('choose_random:{}:{}'.format(*command))

    def delete_from_node(self, command):
        """Sends a request to a specified server to delete the key from the DHT"""
        host, port = self.ports['1']
        with Client(host, port) as cli:
            cli.communication('delete:start_from:{}:{}:{}'.format(*command))

    def query(self, command):
        """Queries a random server for the value of a key in the DHT"""
        host, port = self.ports['1']
        if command[1] == '*':
            with Client(host, port) as cli:
                print cli.communication('print_all_data').split(":")[-1]
        else:
            with Client(host, port) as cli:
                print cli.communication('choose_random:{}:-1:-1:{}'.format(*command))

    def query_from_node(self, command):
        """Queries a specified server for the value of a key in the DHT"""
        host, port = self.ports['1']
        server_id = command[1]
        if command[1] == '*':
            self.bad_command()
        else:
            with Client(host, port) as cli:
                print cli.communication('query:start_from:{}:{}:-1:-1:{}'.format(*command))

    def print_help(self, command):
        """Prints the helping message with the list of commands"""
        print '+------------------------------COMMAND LIST---------------------------------------+'
        for key, value in self.help_dict.iteritems():
            print '| {:>10}: {:<15} |'.format(key, value)
        print "+---------------------------------------------------------------------------------+"

    def create_DHT(self, repl,strat):
        """Created the master server of the DHT"""
        k = Server_master('192.168.1.1', repl, strat)
        self.queue.put(('192.168.1.1', k.get_port()))
        k.accept_connection()
        sys.exit()