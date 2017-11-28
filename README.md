# simple-db-dist
Distributed SimpleDb

Now the NodeServer is able to accept incoming connections.

To test it on your own computer, use ant to build the package and run "java -jar dist/simpledb.jar serve PORTNUMBER" to start the server. Now, configuration files are available for port number 8001 and 8002. Use two terminal windows to start two servers simulating distributed database system.

On another terminal, run "java -jar dist/simpledb.jar client local.txt" to run the head node. You can now type queries to get results.

Type "exit" to close the connection.
