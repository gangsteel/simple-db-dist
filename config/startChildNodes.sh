#!/bin/sh
NODE_NUMBER=$1
echo $E_NUMBER
if [[ $# -eq 0 ]]
    then
        echo "Please specify the number of child nodes!!!"
        exit -1
fi


for i in $(seq 1 $NODE_NUMBER)
do
    PORT=$((8000+$i))
    java -jar dist/simpledb.jar serve $PORT >"config/temp/server$PORT.log" 2>&1 &
    echo "Server run on port $PORT"
done
