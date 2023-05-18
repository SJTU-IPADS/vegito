# VEGITO: a fast distributed in-memory HTAP system

VEGITO is a fast distributed in-memory hybrid transactional/analytical processing (HTAP) system that retrofits fault-tolerant backups without compromising high availability. VEGITO modifies one of the backup replicas for recovery (backup/TP) as a backup replica for analytical query processing (backup/AP). To provide millions of transactions per second and a sub-millisecond freshness, VEGITO re-designs the logging mechanism, storage and index. VEGITO uses a distributed epoch to guarantee the consistency and visibility of backup/AP replicas to analytical queries.

## Graph Extention: GART
GART is an in-memory system extended from HTAP systems for hybrid transactional
and graph analytical processing (HTGAP).
GART should fulfill two unique goals not encountered by HTAP systems.

- To adapt to rich workloads flexibility, GART proposes transparent data
model conversion by graph extraction interfaces, which define rules of
relational-graph mapping.
- To ensure the performance of graph analytical processing (GAP), GART proposes an efficient dynamic graph storage with good locality that stems from
key insights into HTGAP workloads, including
(1) an efficient and mutable compressed sparse row (CSR) representation to
guarantee the locality of scanning edges,
(2) a coarse-grained MVCC to reduce the temporal and spatial overhead of
versioning,
and (3) a flexible property storage to efficiently run various GAP workloads.

## Feature Highlights

- High-performance & good freshness HTAP engines
- Consistent and parallel log cleaning mechanism
- Locality-preserving multi-version column storage
- Two-phase concurrent index updating mechanism
- Graph extension (**GART**), including model conversion interfaces and the
  efficient dynamic graph storage

## Building and Usages

- Please see [`docs/get-started.md`](docs/get-started.md) for details.

- Please see [`docs/micro.md`](docs/micro.md) for micro-benchmarks.


## License

VEGITO is released under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

If you use VEGITO for HTAP in your research, please cite our paper:
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

If you use GART for HTGAP in your research, please cite our paper:
```
  [TBD]
```


## Academic and Conference Papers

[**OSDI' 21**] [Retrofitting High Availability Mechanism to Tame Hybrid Transaction/Analytical Processing](https://www.usenix.org/conference/osdi21/presentation/shen). Sijie Shen, Rong Chen, Haibo Chen, Binyu Zang. The 15th USENIX Symposium on Operating Systems Design and Implementation, Santa Clara, CA, US, July 2021.

[**USENIX ATC' 23**] [Bridging the Gap between Relational OLTP and Graph-based OLAP](https://www.usenix.org/conference/atc23/presentation/shen). Sijie Shen, Zihang Yao, Lin Shi, Lei Wang, Longbin Lai, Qian Tao, Li Su, Rong Chen*, Wenyuan Yu, Haibo Chen, Binyu Zang, and Jingren Zhou. 2023 USENIX Annual Technical Conference, Boston, MA, US, July 2023.
