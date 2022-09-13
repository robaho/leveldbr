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

```
insert time  1000000 records =  111719 ms, usec per op  111.719936
close time  1356 ms
scan time  2779 ms, usec per op  2.779619
scan time 50%  1238 ms, usec per op  2.476208
random access time  93.91525 us per get
insert batch time  1000000 records =  2814 ms, usec per op  2.814384
close time  815 ms
scan time  2891 ms, usec per op  2.891363
scan time 50%  1212 ms, usec per op  2.42586
random access time  96.80413 us per get
```
