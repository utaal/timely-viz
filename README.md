# Receivers for timely-dataflow logging

<br/>
<br/>
<br/>
<br/>

**NOTE: We're in the process of deprecating these tools in favour of more robust diagnostic infrastructure at [timelydataflow/diagnostics](https://github.com/timelydataflow/diagnostics).**

<br/>
<br/>
<br/>
<br/>

## Simple example

Run the simple example with:

```
$ cargo run --example basic -- 4
```

where 4 should be the number of workers in the computation we want to examine. Once that's up and running, you can start the computation to examine (with a recent-enough timely-dataflow):

```
TIMELY_WORKER_LOG_ADDR="127.0.0.1:8000" cargo run ... -- -w 4
```

(or, in Windows: )

```
set TIMELY_WORKER_LOG_ADDR=127.0.0.1:8000
cargo run ... -- -w 4
```

## Drawing the graph

The source is in `src/bin/graph.rs`. Run with:

```
$ cargo run --bin graph -- 4
```

Open a browser at http://localhost:9000
then start the computation we're examining (as before).

## Bar-chart of execution times

The source is in `src/bin/schedule.rs`. Run with:

```
$ cargo run --bin schedule -- 4
```

Open a browser at http://localhost:9000
then start the computation we're examining (as before).


## Steps to run this program
1. create a new directory/folder
2. go to created directory and  git clone https://github.com/frankmcsherry/differential-dataflow.git
3. git clone https://github.com/utaal/timely-viz.git
4. $ cd timely-viz
5. timely-viz$ cargo run --bin dashboard -- 2 html/dashboard.html
6. open a browser on http://localhost:9000/
7. open a new terminal and go to the differential dataflow directory
8. differential-dataflow$ TIMELY_WORKER_LOG_ADDR="127.0.0.1:8000" cargo run --release --example bfs -- 1000 1000 100 1000 no -w2 (we are using bfs example, you can give any timely computation)
