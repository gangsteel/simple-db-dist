#!/bin/bash

query="JOIN(SCAN(test.0),SCAN(test.1),0=0)"
bash ./config/query_running_scripts/run_query.sh $query
