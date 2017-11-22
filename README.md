# simple-db-dist
Distributed SimpleDb

Now the NodeServer is able to accept incoming connections.
(of course, query processing is not implemented yet, and some garbage is returned instead)

To test it on your own computer, find NodeServer class under simpledb package,
run it as an application (the main function) in Eclipse or whatever.

Now you can login the server using telnet from your own computer:

telnet localhost 4444

"4444" is the temporary port number we are using now

And you can type query in the format described in querytree/Query.g

If your query is not parsed, an error message will be returned.

Type "exit" to close the connection.
