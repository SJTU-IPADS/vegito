# Run micro-benchmark
## Edge scan performance of different graph topology storages
We use config files in `configs/micro` to run edge scan on different graph topology storages

```
$ cd build
$ ./run.sh 0 config_file_path micro
```
The values of `config_file_path` for different configurations are:
| Dataset | Graph Storage | Load Pattern | config_file_path
| ---- | ---- | ---- | ---- |
| TPC-C | CSR | bulk load | ../configs/micro/ch-csr-seq.xml |
| TPC-C | LiveGraph | bulk load | ../configs/micro/ch-lg-seq.xml |
| TPC-C | SegCSR | bulk load | ../configs/micro/ch-seg-seq.xml |
| TPC-C | CSR | random insertion | ../configs/micro/ch-csr-ran.xml |
| TPC-C | LiveGraph | random insertion | ../configs/micro/ch-lg-ran.xml |
| TPC-C | SegCSR | random insertion | ../configs/micro/ch-seg-ran.xml |
| SNB-SF-10 | CSR | bulk load | ../configs/micro/ldbc-csr-seq.xml |
| SNB-SF-10 | LiveGraph | bulk load | ../configs/micro/ldbc-lg-seq.xml |
| SNB-SF-10 | SegCSR | bulk load | ../configs/micro/ldbc-seg-seq.xml |
| SNB-SF-10 | CSR | random insertion | ../configs/micro/ldbc-csr-ran.xml |
| SNB-SF-10 | LiveGraph | random insertion | ../configs/micro/ldbc-lg-ran.xml |
| SNB-SF-10 | SegCSR | random insertion | ../configs/micro/ldbc-seg-ran.xml |
| RMAT | CSR | bulk load | ../configs/micro/rmat-csr-seq.xml |
| RMAT | LiveGraph | bulk load | ../configs/micro/rmat-lg-seq.xml |
| RMAT | SegCSR | bulk load | ../configs/micro/rmat-seg-seq.xml |
| RMAT | CSR | random insertion | ../configs/micro/rmat-csr-ran.xml |
| RMAT | LiveGraph | random insertion | ../configs/micro/rmat-lg-ran.xml |
| RMAT | SegCSR | random insertion | ../configs/micro/rmat-seg-ran.xml |
| UK-2005 | CSR | bulk load | ../configs/micro/uk-csr-seq.xml |
| UK-2005 | LiveGraph | bulk load | ../configs/micro/uk-lg-seq.xml |
| UK-2005 | SegCSR | bulk load | ../configs/micro/uk-seg-seq.xml |
| UK-2005 | CSR | random insertion | ../configs/micro/uk-csr-ran.xml |
| UK-2005 | LiveGraph | random insertion | ../configs/micro/uk-lg-ran.xml |
| UK-2005 | SegCSR | random insertion | ../configs/micro/uk-seg-ran.xml |
| Twitter-2010 | CSR | bulk load | ../configs/micro/tw-csr-seq.xml |
| Twitter-2010 | LiveGraph | bulk load | ../configs/micro/tw-lg-seq.xml |
| Twitter-2010 | SegCSR | bulk load | ../configs/micro/tw-seg-seq.xml |
| Twitter-2010 | CSR | random insertion | ../configs/micro/tw-csr-ran.xml |
| Twitter-2010 | LiveGraph | random insertion | ../configs/micro/tw-lg-ran.xml |
| Twitter-2010 | SegCSR | random insertion | ../configs/micro/tw-seg-ran.xml |
| Wiki | CSR | bulk load | ../configs/micro/wiki-csr-seq.xml |
| Wiki | LiveGraph | bulk load | ../configs/micro/wiki-lg-seq.xml |
| Wiki | SegCSR | bulk load | ../configs/micro/wiki-seg-seq.xml |
| Wiki | CSR | random insertion | ../configs/micro/wiki-csr-ran.xml |
| Wiki | LiveGraph | random insertion | ../configs/micro/wiki-lg-ran.xml |
| Wiki | SegCSR | random insertion | ../configs/micro/wiki-seg-ran.xml |

Switch between SegCSR (ON) and SegCSR/TS (OFF):
```
$ cmake ../ -DENABLE_EPOCH=[ON/OFF]
```

The system will load a specific dataset in a specific pattern to a specific graph topology storage, and scan edges. System will print edge scan time per round, write throughput and memory usage.

## Performance of different property storage

- Read performance
    - Command is `./run.sh 0 gart-ap-tpcc.xml ch`
    - In config file: `query_threads` = 1
    - In config file: `bstore_type` = 1 for row-store, `bstore_type` = 1 for column-store
    - In config file: `ch.query` = 29
    - You should modify the values of the variables `test_flex_col` and `merge_cols` in the file `src/app/ch/micro_query/flex_prop_read.cc` to set the property storage configuration

- Write performance
    - In config file: `ch.query` = 30
    - Other configurations are the same as above