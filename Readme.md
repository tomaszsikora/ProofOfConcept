# DirectCache #
It is a proof concept for large off heap cache on java direct ByteBuffers.
Current version is using Trove TIntLongHashMap for indexing position in buffer.
Limitations:
    2^16 - max size of single record
    2^48 - max size of cache - 262144 GB

## Performance ##

### Test machine ###
* AMD FX-4100 3.6GHz
* 8GB Ram
* Windows 7 Pro 64bit

-ea -Xmx4g -XX:MaxDirectMemorySize=3g

Test case:

Put 25_000_000 records (28 bytes long)
Get 25_000_000 records

Results:

Avg put: 172ns Throughput: 166MB/s
Avg get: 88ns Throughput: 333MB/s


## How to use it ##

If you want to use direct memory in your application, set enough direct memory for JVM
i.e.: -XX:MaxDirectMemorySize=3g

## Note! ##

This structure currently is not Thread Safe!
