#!/usr/bin/env python
import sys

from dht import DHT

def main():
    """
    DHT

    Creates the DHT
    Creates a basic CLI for user input
    """
    if len(sys.argv) != 3 :
        print 'To create the DHT you need to give 2 parameters the replication size and the consistency stategy'
    elif sys.argv[2] != 'linear' and sys.argv[2] != 'eventual':
        print 'We only support linear and eventual consistency'
    elif int(sys.argv[1]) < 0 :
        print 'Replication size must be a positive integer.'
    else :
        # Create the DHT
        distrib_hash_table = DHT(int(sys.argv[1]),sys.argv[2])
        # Run a basic CLI
        distrib_hash_table.cli()

if __name__ == '__main__':
    main()