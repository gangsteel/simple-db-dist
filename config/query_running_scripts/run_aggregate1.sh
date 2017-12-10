#!/bin/bash

query="AGGREGATE(SCAN(test.0),0,COUNT)"
bash ./config/query_running_scripts/run_query.sh $query
