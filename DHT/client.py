#!/usr/bin/env python

import socket
import sys

class Client(object):
    def __init__(self, IP, PORT):
        self.PORT=PORT
        self.answer = ''
        self.client_socket=socket.socket()
        if PORT!=-1:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((IP, int(PORT)))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __enter__(self):
        return self

    def close(self):
        try:
            self.client_socket.send('quit')
        except socket.error:
            sys.stderr.write('client: CLOSURE WAS UNSUCCESSFUL')
            sys.exit()
        else:
            self.client_socket.recv(16384)
            self.client_socket.close()

        
    def communication(self,message):
        try:
            self.client_socket.send(message)
        except socket.error:
            sys.stderr.write('client: SEND MESSAGE FAIL')
            sys.exit()
          
        try:
            self.answer = self.client_socket.recv(16384)
        except socket.error:
            sys.stderr.write('client: READ MESSAGE FAIL '+ str(self.IP) + ":" +str(self.PORT))
            sys.exit()
        else:
            return self.answer



