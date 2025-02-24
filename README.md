# Map Reduce Implementation in GO

This project is done by following instructions provided by the projects section of MIT 6.824

## Running the program

There are two main functions needed for the mapreduce to function i.e the worker process and the coordinator process

You need to run both the worker and the coordinator as separate process as follows 

```go
go run ./mrcoordinator.go pg-*.txt
```

here pg-*.txt are all the user input files 

```go
go run ./mrworker.go ./wc.so
```

the `./wc.so` file can be obtain by running the following command

```go
go build -buildmode=plugin ./wc.go
```

