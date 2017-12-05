#!/bin/bash

LINE_NUMBER=$1
COLUMN_NUMBER=$2
RANGE=$3
FILE_NAME=$4
SPLIT_LINE_NUMBER=$5

bash ./randFile.sh $1 $2 $3 "temp/$FILE_NAME.1.txt"
split -l $SPLIT_LINE_NUMBER "temp/$FILE_NAME.1.txt" "temp/$FILE_NAME.1"

bash ./randFile.sh $1 $2 $3 "temp/$FILE_NAME.2.txt"
split -l $SPLIT_LINE_NUMBER "temp/$FILE_NAME.2.txt" "temp/$FILE_NAME.2"

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
    mv "temp/$FILE_NAME.1$APPEND" "temp/$FILE_NAME.1$APPEND.txt"
    java -jar ../dist/simpledb.jar convert "temp/$FILE_NAME.1$APPEND.txt" $COLUMN_NUMBER $TYPE_STRING
    mv "temp/$FILE_NAME.1$APPEND.dat" "temp/$FILE_NAME.1$FILE_INDEX.dat"

    mv "temp/$FILE_NAME.2$APPEND" "temp/$FILE_NAME.2$APPEND.txt"
    java -jar ../dist/simpledb.jar convert "temp/$FILE_NAME.2$APPEND.txt" $COLUMN_NUMBER $TYPE_STRING
    mv "temp/$FILE_NAME.2$APPEND.dat" "temp/$FILE_NAME.2$FILE_INDEX.dat"

    if [[ $FILE_INDEX -ge $FILE_NUMBER ]]
    then
        break
    fi
    (( FILE_INDEX++ ))
done

# Start to distribute the dat files
echo "SimpleDb format compilation completed, start distributing the files"
# Prepare catalog.txt
CATALOG_TEXT="$FILE_NAME.1 (f1 int"
for i in $(seq 2 $COLUMN_NUMBER)
do
    CATALOG_TEXT+=", f$i int"
done 
CATALOG_TEXT+=")"
echo $CATALOG_TEXT > temp/catalog.txt

CATALOG_TEXT="$FILE_NAME.2 (f1 int"
for i in $(seq 2 $COLUMN_NUMBER)
do
    CATALOG_TEXT+=", f$i int"
done 
CATALOG_TEXT+=")"
echo $CATALOG_TEXT >> temp/catalog.txt

> head/local.txt

for INDEX in $(seq 1 $FILE_NUMBER)
do
    DIRECTORY_NAME="child/$((8000+$INDEX))"
    mkdir -p $DIRECTORY_NAME
    cp "temp/$FILE_NAME.1$INDEX.dat" "$DIRECTORY_NAME/$FILE_NAME.1.dat"
    cp "temp/$FILE_NAME.2$INDEX.dat" "$DIRECTORY_NAME/$FILE_NAME.2.dat"
    cp temp/catalog.txt "$DIRECTORY_NAME/"
    echo "127.0.0.1:$((8000+$INDEX))" >> head/local.txt
done
