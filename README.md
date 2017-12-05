# simple-db-dist
Distributed SimpleDb

Now the NodeServer is able to accept incoming connections.

To test it on your own computer, use ant to build the package and run "java -jar dist/simpledb.jar serve PORTNUMBER" to start the server. Now, configuration files are available for port number 8001 and 8002. Use two terminal windows to start two servers simulating distributed database system.

On another terminal, run "java -jar dist/simpledb.jar client local.txt" to run the head node. You can now type queries to get results.

Type "exit" to close the connection.

*** BENCHMARKING OF RANDOM TABLES ON SINGLE MACHINE (DIFFERENT PORTS) ***
Some shell scripts are provided to facilitate benchmarking. Here are the steps:

1) cd to config directory, run setup.sh (you can edit this file to specify row number, column number, random number range, table name, and row number in each child node of the configuration)

2) now corresponding data files, catalog files on child nodes, and running configuration on head node should be correctly put in the directories. cd to the root directory of the package (cd ..), run "./config/runLocal.sh NUMBER_OF_NODES", where NUMBER_OF_NODES should be the number of child nodes appear in the terminal (Number of files generated). Failing to add this argument or having the wrong number will result in exceptions. ** We might want to make the shell script automatically determine number of child nodes **

3) Now the servers should be running. After 5 seconds, the client (head node) should be running. Now you can type in queries. e.g. SCAN(test.1), SCAN(test.2), FILTER(SCAN(test.1), 1>8), JOIN(SCAN(test.1), SCAN(test.2), 0=0), ...

4) Don't forget to cleanup. First, run "killall java" to stop the servers. Then, cd to config directory and run cleanup.sh to clean up the data files. ** some automation here as well? **
