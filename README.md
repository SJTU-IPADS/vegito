# VEGITO: a fast distributed in-memory HTAP system

VEGITO is a fast distributed in-memory HTAP system that retrofits fault-tolerant backups without compromising high availability. VEGITO modifies one of the backup replicas for recovery (backup/TP) as a backup replica for analytical query processing (backup/AP). To provide millions of transactions per second and a sub-millisecond freshness, VEGITO re-designs the logging mechanism, storage and index. VEGITO uses a distributed epoch to guarantee the consistency and visibility of backup/AP replicas to analytical queries.

## Feature Highlights

- High-performance & good freshness HTAP engines
- Consistent and parallel log cleaning mechanism 
- Locality-preserving multi-version column storage
- Two-phase concurrent index updating mechanism
- Support (static) graph index

## Features not supported yet

This codebase has the basic functionality of VEGITO, including basic computation engines, storage, and static benchmark code of CH-benCHmark. Other features will be released soon.

## Building and Usages

Please see [`docs/get-started.md`](docs/get-started.md) for details.

## License

VEGITO is released under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

If you use VEGITO in your research, please cite our paper:
```    
  @inproceedings {shen2021vegito,
    author = {Sijie Shen and Rong Chen and Haibo Chen and Binyu Zang},
    title = {Retrofitting High Availability Mechanism to Tame Hybrid Transaction/Analytical Processing},
    booktitle = {15th {USENIX} Symposium on Operating Systems Design and Implementation ({OSDI} 21)},
    year = {2021},
    isbn = {978-1-939133-22-9},
    pages = {219--238},
    url = {https://www.usenix.org/conference/osdi21/presentation/shen},
    publisher = {{USENIX} Association},
    month = jul,
  }
```

## Academic and Conference Papers

[**OSDI**] Retrofitting High Availability Mechanism to Tame Hybrid Transaction/Analytical Processing. Sijie Shen, Rong Chen, Haibo Chen, Binyu Zang. The 15th USENIX Symposium on Operating Systems Design and Implementation, Santa Clara, CA, US, July 2021.
