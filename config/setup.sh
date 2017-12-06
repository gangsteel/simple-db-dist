#!/bin/bash

bash ./cleanup.sh
mkdir -p temp
mkdir -p child
mkdir -p head
bash ./randAndSplitTwoTable.sh 10 3 10 test 5
