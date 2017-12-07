#!/bin/bash

query=$1

echo 'Running benchmark on DistributedDb with local machine'
java -jar dist/simpledb.jar distributed ./config/head/local.txt $query | grep 'time'
