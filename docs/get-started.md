# Getting Started Instructions

## Benchmark

- [CH-benCHmark](https://db.in.tum.de/research/projects/CHbenCHmark/index.shtml?lang=en)


## Hardware

To reproduce the experiment results, each machine must have:
1. at least one (two is better) Mellanox RDMA network card (e.g., Mellanox ConnectX-4 MT27700 100Gbps InfiniBand NIC).
2. Intel processors with 2 sockets and Restricted Transactional Memory (RTM)(e.g., Xeon E5-2650 v4). 
The performance isolation depends on the isolation of NICs and sockets.

## Build Dependencies

- Our dependencies are not very complex, but there is some configuration on SSH, since we use SSH to command other machines and collect data.

- Libarary

  - [Boost `1.61.0`](https://www.boost.org/doc/libs/1_61_0/more/getting_started/unix-variants.html) (Only tested.)

  - If you install these libraries locally (e.g., install in `~/local/`), please set the environment variables, like:

    ```
    boost_home="$HOME/local/boost"
    export CPATH="$boost_home/include:$CPATH"
    export LD_LIBRARY_PATH="$boost_home/lib:$LD_LIBRARY_PATH"
    export LIBRARY_PATH="$boost_home/lib:$LIBRARY_PATH"
    ```

- [MLNX_OFED driver](https://www.mellanox.com/products/infiniband-drivers/linux/mlnx_ofed)

- gcc-8, g++-8

  ```
  sudo apt-get software-properties-common
  sudo add-apt-repository ppa:ubuntu-toolchain-r/test
  sudo apt-get update 
  sudo apt-get install gcc-8 g++-8
  ```

- SSH

  - We run distributed systems by `ssh`

  - Please set the `~/.ssh/enviroment` after configuring the libraries: 

    ```
    # you need to create ~/.ssh/environment at first
    echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH" >> ~/.ssh/environment
    ```

### Check the environment

This step helps you to check the SSH environment and the RDMA environment.

You can use `ssh <hostname/ip>` or `ssh -p 52022 <hostname/ip>` if in docker to connect other machines by SSH.

You can use the following test to check the RDMA:

```
# Server
ib_write_bw -p 23333 -a -d mlx5_1 &

# Client
ib_write_bw -p 23333 -a -F $server_IP -d mlx5_1 --report_gbits
```

## Build it

Before you build it, you need to modify two files as your configuration:

- `src/arch.h`, please update your CPU/NIC information like `lscpu`. Note that we use the physical cores without hyper-thread.

We do **not allow** building in the source directory. Suppose we build in a separate directory:

```
$ mkdir build
$ cd build
$ cmake ../ -DCMAKE_BUILD_TYPE=[none/debug]
$ make -jN
```

After `make` there will be some files under `build/`: 
- `vegito` that is executable
- `run.sh` and `scripts/` that are scripts to run benchmark

## Run a Hello-World demo

We use config files to run `vegito`. The first demo uses `configs/hello-world.xml`. You need to modify this config file in the `mapping` scope:

```
 <servers>
    <num> 1 </num>
    <mapping>
      <a> mac00 </a>
      <a> mac01 </a>
      ...
```

Please change the `mac00`, `mac01`, `...` to the ip or hostname of machines in your cluster. The `num` means how many machines you use in this demo.

```
$ cd build
$ cp ../configs/hello-world.xml .
$ ./run.sh 8 hello-world.xml
```

In this demo, you run a simple `vegito` without replication in a single machine. And we use 8 transaction threads and 4 log cleaners.

You can see:

```
@01 System thr 0.000000 M, log: send 0.000000 M clean 0.000000 M, queries 0.000000, read thpt 0.000000 M
  txn epoch 67, read epoch 0, freshness -nan ms
@02 System thr 0.504996 M, log: send 0.000000 M clean 0.000000 M, queries 0.000000, read thpt 0.000000 M
  txn epoch 100, read epoch 0, freshness -nan ms
@03 System thr 0.506893 M, log: send 0.000000 M clean 0.000000 M, queries 0.000000, read thpt 0.000000 M
  txn epoch 134, read epoch 0, freshness -nan ms
...
```

Then, you can modify the `num` in 2, 3, ... to run the distributed `vegito`.

## Examples

We list methods of example evaluations.

You need the copy the specified configuration under `./configs` to `./build` and run by `./run.sh <num-tp-threads> <config>`. Please modify the hostname in each config file.

In each evaluation, we will run each data point for 20 seconds, and you only need to read the final results before finishing, especially `OLTP thr` (OLTP throughput)  and `OLAP thr`  (OLAP throughput) .

```
--- benchmark statistics ---
  (1) data cnt: 18
  (2) OLTP fly: 10, OLTP thr: 1.922732 M txn/s (med 1.893286) lat: 46.920597 us
  (3) OLAP fly: 9, OLAP thr: 29.455692 qry/s lat: 0.000000 ms
```

### OLTP-specific workloads

- Throughput & Latency

  - Use `vegito-all-oltp.xml`
  - `num-tp-threads` = (total cores - 2) / 3 * 2 (**"total cores" is the number of cores on each machine.**)
  - In config file: `backup_threads` =  (total cores - 2) / 3
  - For thr-lat relation: In config file: Tune the number of `on_fly` (client ratio) from 1-40 to show the relation between `OLTP thr`and `OLTP lat`

- Scalability

  - Use `vegito-all-oltp.xml`
  - `num-tp-threads` = (total cores - 2) / 3 * 2
  - In config file:`backup_threads` =  (total cores - 2) / 3
  - In config file: `on-fly` = 30, or `client` = 0
  - Tune the number of `<servers.num>` from 3-16 to show the scalability of Vegito

### OLAP-specific workloads 

  - Use `vegito-all-olap.xml`

  - In config file: `query_threads = query_session` = total cores - 2

  - The latency of each queries will report at last before `benchmark statistics`, eg

    ```
    Q01: executed 5, avg db sz 3600092,      walk 0,         latency 44.696125 ms
    Q02: executed 5, avg db sz 0,    walk 0,         latency 171.841766 ms
    ...
    ```

### HTAP workloads

- OLAP interference on OLTP

  - Command is `./run.sh <num-tp-threads> vegito-htap.xml`
  - fix the command `num-tp-threads` = (total cores / 2) / 3 * 2 (half of cores)
  - In config file:`backup_threads` =  (total cores / 2) / 3
  - In config file: `query_threads = query_session` = total cores / 2 - 2
  - In config file: `client` = 0
  - In config file: `q_client` = 1, add `q_on_fly` from 0 to 20

- OLTP interference on OLAP

  - Command is `./run.sh <num-tp-threads> vegito-htap.xml`
  - fix the command `num-tp-threads` = (total cores / 2) / 3 * 2 (half of cores)
  - In config file:`backup_threads` =  (total cores / 2) / 3
  - In config file: `query_threads = query_session` = total cores / 2 -2
  - In config file: `q_client` = 0
  - In config file: `client` = 1, add `on_fly` from 0 to 20

### Freshness

- How does the epoch interval affect the freshness?
  - Modify `framework/framework_cfg.h` (line 8), set `FRESHNESS = 1`, compile
  - `./run.sh <num-tp-threads> freshness.xml`
  - fix the command `num-tp-threads` = (total cores / 2) / 3 * 2 (half of cores)
  - In config file: tune `sync_ms` from 5 to 50, step is 5
  - Redirect the output to a file, then collect the freshness
- How does the epoch interval affect the performance of OLTP?
  - Modify `framework/framework_cfg.h` (line 8), set `FRESHNESS = 0`, compile
  - `./run.sh <num-tp-threads> freshness.xml`
  - fix the command `num-tp-threads` = (total cores / 2) / 3 * 2 (half of cores)
  - In config file: tune `sync_ms` from 5 to 50, step is 5

- Usage: `./scripts/run-fresh.py <template-xml> <epoch-interval-ms>`. It means use a template XML and set the epoch interval as ms. It will create two logs: `freshness-<epoch-interval-ms>-raw.log` and `freshness-<epoch-interval-ms>.log` (`grep` the freshness information from the raw log). And give you the results of freshness directly.
- We provide `./scripts/fresh-eval.py` to parse the logs `freshness-*.log` (**not the raw logs**).

### Static Graph index
- Now support static graph index on ORDERS->ORDERLINE
- Set the macro `OL_GRAPH 1` in `src/app/ch/ch_query.h`
- Modify the xml config file (sample in `configs/gindex.xml`)
- Usage: 
```
$ mkdir build; cd build
$ cmake ..; make -j
$ cp ../configs/gindex.xml .
$ ./run.sh 0 gindex.xml
```
