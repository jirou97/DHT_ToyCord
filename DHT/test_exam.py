#!/usr/bin/env python

import sys
import os
import time
from multiprocessing import Process 

from dht import DHT




def test_custom(dht):
    dht.execute(['insert','Apple','1'])
    dht.execute(['insert','Pear','2'])
    dht.execute(['insert','Orange','3'])
    dht.execute(['insert','Saguini','4'])
    dht.execute(['insert','Pineapple','5'])
    dht.execute(['insert','Cherimoya','6'])
    dht.execute(['insert','Durian','7'])
    dht.execute(['insert','Kiwano','8'])
    dht.execute(['insert','Tamarillo','9'])
    dht.execute(['insert','Salak','10'])
    dht.cli()

if __name__ == '__main__':
    
    #Create DHT and join 10 nodes
    dht = DHT(2,'linear')
    #dht = DHT(int(sys.argv[1]),sys.argv[2])
    print "DHT created"
    p2 =Process(target = lambda : os.system('./start_servers.sh node2 30002'))
    p2.start()
    time.sleep(0.5)
    print "node 2 joined"
    p3 =Process(target = lambda : os.system('./start_servers.sh node3 30003'))
    p3.start()
    print "node 3 joined"
    time.sleep(1)

    #Run the test
    test_custom(dht)

    #terminate the processes 
    p2.terminate()
    p3.terminate()