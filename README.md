# Receivers for timely-dataflow logging

## Simple example

Run the simple example with:

```
$ cargo run --example basic -- 4
```

where 4 should be the number of workers in the computation we want to examine. Once that's up and running, you can start the computation to examine (with a recent-enough timely-dataflow):

```
TIMELY_WORKER_LOG_ADDR="127.0.0.1:8000" cargo run ... -- -w 4
```

## Drawing the graph

The source is in `src/bin/graph.rs`. Run with:

```
$ cargo run --bin graph -- 4
```

Open a browser at http://localhost:9000
then start the computation we're examining (as before).
