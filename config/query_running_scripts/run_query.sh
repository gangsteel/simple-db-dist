#!/bin/bash

query=$1

bash ./config/query_running_scripts/run_single_distributed.sh $query

echo 'Wait a few seconds before running on SimpleDb'
sleep 3

bash ./config/query_running_scripts/run_simpledb.sh $query

echo 'Killing all child nodes'
bash ./config/killChildNodes.sh

echo 'Done'
