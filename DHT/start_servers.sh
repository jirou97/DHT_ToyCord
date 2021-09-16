SCRIPT="cd DHT; python server.py $2;"
ssh -l user $1 "${SCRIPT}"
