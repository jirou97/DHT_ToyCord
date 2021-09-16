#!/bin/bash

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
scp -r ~/.ssh/ user@node0:~/
scp -r ~/.ssh/ user@node1:~/
scp -r ~/.ssh/ user@node2:~/
scp -r ~/.ssh/ user@node3:~/
scp -r ~/.ssh/ user@node4:~/
