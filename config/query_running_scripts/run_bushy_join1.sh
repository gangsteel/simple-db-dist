#!/bin/bash

query="AGGREGATE(JOIN(JOIN(SCAN(test.0),SCAN(test.1),0=0),JOIN(SCAN(test.2),SCAN(test.3),0=0),0=0),0,COUNT)"
bash ./config/query_running_scripts/run_query.sh $query
