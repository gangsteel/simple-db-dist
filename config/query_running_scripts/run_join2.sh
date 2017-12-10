#!/bin/bash

query="JOIN(JOIN(SCAN(test.0),SCAN(test.1),0=0),SCAN(test.2),0=0)"
bash ./config/query_running_scripts/run_query.sh $query
