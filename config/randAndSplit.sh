#!/bin/bash

LINE_NUMBER=$1
COLUMN_NUMBER=$2
RANGE=$3
FILE_NAME=$4
SPLIT_LINE_NUMBER=$5

bash ./randFile.sh $1 $2 $3 "temp/$FILE_NAME.txt"
split -l $SPLIT_LINE_NUMBER "temp/$FILE_NAME.txt" "temp/$FILE_NAME"

FILE_NUMBER=$((($LINE_NUMBER+$SPLIT_LINE_NUMBER-1)/$SPLIT_LINE_NUMBER))
echo "Generated $FILE_NUMBER files, start converting to SimpleDb format..."

FILE_INDEX=1
TYPE_STRING="int"
for i in $(seq 2 $COLUMN_NUMBER)
do
    TYPE_STRING+=",int"
done

for APPEND in {a..z}{a..z}
do
    # echo $FILE_INDEX
    # echo $APPEND
    # use java to convert txt file to dat file
    mv "temp/$FILE_NAME$APPEND" "temp/$FILE_NAME$APPEND.txt"
    java -jar ../dist/simpledb.jar convert "temp/$FILE_NAME$APPEND.txt" $COLUMN_NUMBER $TYPE_STRING
    mv "temp/$FILE_NAME$APPEND.dat" "temp/$FILE_NAME$FILE_INDEX.dat"
    if [[ $FILE_INDEX -ge $FILE_NUMBER ]]
    then
        break
    fi
    (( FILE_INDEX++ ))
done
