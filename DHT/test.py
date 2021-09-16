#!/usr/bin/env python

import sys
import os
import time
from multiprocessing import Process 

from dht import DHT



def test_insert_query(dht):
    """
    Create the DHT and add 10 nodes
    Then insert file, query file and destroy it.    
    """
    dht.execute(['overlay'])
    dht.execute(['insertfile','insert.txt'])
    dht.execute(['queryfile','query.txt'])
    #dht.execute(['query','*'])
    time.sleep(1)

    dht.execute(['exit'])

def test_join_depart(dht):
    """
    Create the DHT and add 10 nodes
    1.
        Insert files from insert.txt in DHT
    2.
        Then node 11 joins the DHT.
        This node has to take the keys from its next node, that no longer belong to it.
        The joined node also has to take the keys from the previous k-1 nodes as replicas.
        Some replicas have to be deleted from the next nodes of node 11.
    3.
        Node11 departs from the DHT. 
        Its keys need to be redistributed to its next node.
        The replicas has to be forwarded as well
    
    """
    dht.execute(['overlay'])
    dht.execute(['insertfile','insert.txt'])
    dht.execute(['query','*'])
    CSI = "\x1B["
    print CSI +"36;40m node 11 joining" + CSI +'0m\n'
    p11 =Process(target = lambda : os.system('./start_servers.sh node2 30015'))
    p11.start()
    # wait 2 seconds for the changes to be visible..
    time.sleep(2)
    dht.execute(['overlay'])
    dht.execute(['query','*'])
    print CSI +"36;40m node 11 departing" + CSI +'0m\n'
    dht.execute(['depart',11])
    time.sleep(5)
    dht.execute(['overlay'])
    dht.execute(['query','*'])
    dht.execute(['exit'])
    p11.terminate()

def test_parsefile(dht ):
    """
    Create the DHT and add 10 nodes
    Run the commands in requests.txt and destroy the DHT.  
    """
    dht.execute(['overlay'])
    dht.execute(['parsefile','requests.txt'])
    time.sleep(1)
    dht.execute(['exit'])

def test_insert_update(dht ):
    #check_freshness
    """
    Create the DHT and add 10 nodes
    Run the commands in requests.txt and destroy the DHT.  
    """
    #overlay
    #->192.168.1.1/30001 id=1
    #   ->192.168.1.3/30005 id=5
    #       ->192.168.1.2/30003 id=3
    #           ->192.168.1.5/30010 id=10
    #               ->192.168.1.5/30009 id=9
    #               ->192.168.1.3/30006 id=6
    #           ->192.168.1.2/30004 id=4
    #       ->192.168.1.1/30002 id=2
    #   ->192.168.1.4/30008 id=8
    #->192.168.1.4/30007 id=7->
    dht.execute(['overlay'])
    dht.execute(['insert','a','3'])
    Process(target = dht.execute(['query_from','3','a'])).start()
    Process(target = dht.execute(['query_from','5','a'])).start()
    Process(target = dht.execute(['query_from','1','a'])).start()
    Process(target = dht.execute(['insert_from','5','a','5'])).start()
    Process(target = dht.execute(['query_from','9','a'])).start()
    Process(target = dht.execute(['query_from','3','a'])).start()
    Process(target = dht.execute(['query_from','5','a'])).start()
    Process(target = dht.execute(['query_from','1','a'])).start()
    dht.execute(['exit'])

def test_insert_delete(dht ):
    """
    Create the DHT and add 10 nodes
    Run the commands in requests.txt and destroy the DHT.  
    """
    #overlay
    #->192.168.1.1/30001 id=1
    #   ->192.168.1.3/30005 id=4
    #       ->192.168.1.2/30003 id=3
    #           ->192.168.1.5/30010 id=10
    #               ->192.168.1.5/30009 id=9
    #               ->192.168.1.3/30006 id=6
    #           ->192.168.1.2/30004 id=5
    #       ->192.168.1.1/30002 id=2
    #   ->192.168.1.4/30008 id=8
    #->192.168.1.4/30007 id=7->
    dht.execute(['overlay'])
    dht.execute(['insert','a','3'])
    Process(target = dht.execute(['query_from','3','a'])).start()
    print('Deleting a...')
    Process(target = dht.execute(['delete_from','5','a'])).start()
    Process(target = dht.execute(['query_from','3','a'])).start()
    Process(target = dht.execute(['query_from','5','a'])).start()
    Process(target = dht.execute(['query_from','1','a'])).start()
    dht.execute(['exit'])

def test_query_all (dht):
    dht.execute(['overlay'])
    dht.execute(['query','*'])
    dht.execute(['insertfile','insert.txt'])
    dht.execute(['query','*'])
    dht.execute(['exit'])

def test_custom(dht):
    dht.cli()

if __name__ == '__main__':

    #Create DHT and join 10 nodes
    dht = DHT(int(sys.argv[1]),sys.argv[2])
    print "DHT created"
    p2 =Process(target = lambda : os.system('python server.py 30002'))
    p2.start()
    time.sleep(0.5)
    print "node 2 joined"
    p3 =Process(target = lambda : os.system('./start_servers.sh node1 30003'))
    p3.start()
    time.sleep(0.5)
    print "node 3 joined"
    p4 =Process(target = lambda : os.system('./start_servers.sh node1 30004'))
    p4.start()
    time.sleep(0.5)
    print "node 4 joined"
    p5 =Process(target = lambda : os.system('./start_servers.sh node2 30005'))
    p5.start()
    time.sleep(0.5)
    print "node 5 joined"
    p6 =Process(target = lambda : os.system('./start_servers.sh node2 30006'))
    p6.start()
    time.sleep(0.5)
    print "node 6 joined"
    p7 =Process(target = lambda : os.system('./start_servers.sh node3 30007'))
    p7.start()
    time.sleep(0.5)
    print "node 7 joined"
    p8 =Process(target = lambda : os.system('./start_servers.sh node3 30008'))
    p8.start()
    time.sleep(0.5)
    print "node 8 joined"
    p9 =Process(target = lambda : os.system('./start_servers.sh node4 30009'))
    p9.start()
    time.sleep(0.5)
    print "node 9 joined"
    p10 =Process(target = lambda : os.system('./start_servers.sh node4 30010'))
    p10.start()
    print "node 10 joined"
    time.sleep(1)

    #Run the test
    test_custom(dht)

    #terminate the processes 
    p2.terminate()
    p3.terminate()
    p4.terminate()
    p5.terminate()
    p6.terminate()
    p7.terminate()
    p8.terminate()
    p9.terminate()
    p10.terminate()