#!/bin/bash

LINE_NUMBER=$1
COLUMN_NUMBER=$2
RANGE=$3
FILE_NAME=$4

echo "Generating $LINE_NUMBER lines, $COLUMN_NUMBER columns of random numbers from 0 to $(($RANGE-1))"
echo "Output file name: $FILE_NAME"

> $FILE_NAME

for i in $(seq 1 $LINE_NUMBER)
do
    ROW=$(($RANDOM%$RANGE))
    for j in $(seq 2 $COLUMN_NUMBER)
    do
        ROW+=",$(($RANDOM%$RANGE))"
    done
    echo $ROW >> $FILE_NAME
done
