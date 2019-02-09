##До оптимизаций

##После оптимизаций
###PUT
```
$ wrk --latency -c4 -d10m -s scripts/put.lua http://localhost:8080
Running 10m test @ http://localhost:8080
  2 threads and 4 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     0.98ms    7.21ms 290.26ms   99.68%
    Req/Sec     3.10k   420.74     4.30k    76.17%
  Latency Distribution
     50%  504.00us
     75%  657.00us
     90%    1.03ms
     99%    4.29ms
  3695350 requests in 10.00m, 236.12MB read
Requests/sec:   6158.23
Transfer/sec:    402.93KB
```
###GET
```
$ wrk --latency -c4 -d10m -s scripts/get.lua http://localhost:8080
Running 10m test @ http://localhost:8080
  2 threads and 4 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.42ms   11.22ms 436.61ms   99.53%
    Req/Sec     2.68k   225.57     4.40k    95.89%
  Latency Distribution
     50%  607.00us
     75%    0.89ms
     90%    1.28ms
     99%    4.09ms
  3185103 requests in 10.00m, 490.18MB read
  Non-2xx or 3xx responses: 88023
Requests/sec:   5307.65
Transfer/sec:    836.45KB
```
###PUT-GET
```
$ wrk --latency -c4 -d10m -s scripts/get-put.lua http://localhost:8080
Running 10m test @ http://localhost:8080
  2 threads and 4 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.69ms   14.17ms 474.36ms   99.37%
    Req/Sec     2.86k   293.56     3.56k    87.57%
  Latency Distribution
     50%  557.00us
     75%  821.00us
     90%    1.24ms
     99%    4.59ms
  3401062 requests in 10.00m, 371.48MB read
  Non-2xx or 3xx responses: 35032
Requests/sec:   5668.00
Transfer/sec:    633.95KB
```