#!/bin/bash

NODE_NUMBER=$1

for i in $(seq 1 $NODE_NUMBER)
do
    java -jar dist/simpledb.jar serve $((8000+$i)) >config/temp/serverLog.log &
    echo "Server run on port $((8000+$i))"
done

echo "Start client..."
java -jar dist/simpledb.jar client local.txt
