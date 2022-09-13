**About**

Remote access to leveldb database. Uses Google protobufs and gRPC.

**Notes**

There is a single gRPC stream per open database,
upon which all requests are multiplexed. The stream is shared but requests are completed synchronously (since the server processes an inbound message synchronously) but this may change in the future to allow overlapping requests - that is, asynchronous handling
by the server. For best performance, multiple connections should be made to the server, rather than sharing a database connection.

**To Use**

go run cmd/server

There is a sample command line client in cmd/client which uses the client API.

**Performance**

Using the same 'performance' test as leveldb, but using the remote layer:

<pre>
insert time  1000000 records =  95207 ms, usec per op  95.207138
close time  678 ms
scan time  1930 ms, usec per op  1.930828
scan time 50%  874 ms, usec per op  1.749824
random access time  72.44012 us per get
insert batch time  1000000 records =  1696 ms, usec per op  1.69682
close time  367 ms
scan time  1900 ms, usec per op  1.900483
scan time 50%  982 ms, usec per op  1.964134
random access time  79.19778 us per get
</pre>
