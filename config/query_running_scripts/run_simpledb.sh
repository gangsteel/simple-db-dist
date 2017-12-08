#!/bin/bash

query=$1

echo 'Running benchmark on SimpleDb'
java -jar dist/simpledb.jar simple $query | grep 'time'
