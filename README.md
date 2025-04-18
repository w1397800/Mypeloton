# Mypeloton: An In-Memory OLTP Database Engine Based on MySQL 8.0
## 🧠 System Overview

**Mypeloton** is an in-memory database prototype system built on top of **MySQL 8.0**, designed for high-throughput OLTP workloads.  
It adopts a **Multi-Version Timestamp Ordering (MVTO)** concurrency control protocol and features an **append-only row store** architecture.  
Key components include a **Masstree-based index**, a **parallel logging system**, and an **epoch-based decentralized garbage collector**.
 
Mypeloton supports the **serializable isolation level**, and achieves:

- ⏱️ **45-second recovery time** for a 200-warehouse TPC-C workload
- ⚡ **1.9 million TpmC**, which is **2× higher than InnoDB** under large-buffer configurations


## 🚀 Key Features

- Fully in-memory storage architecture for ultra-low latency
- Custom transaction processing and garbage collection
- Fast recovery and lightweight concurrency control
- Retains compatibility with MySQL 8.0 client ecosystem

## 📜 License

This project is licensed under the [GNU General Public License v2.0](LICENSE).
It is based on the original [MySQL 8.0](https://github.com/mysql/mysql-server), which is also licensed under GPLv2.

All original contributions, especially under the `/storage/peloton` directory,
are created and maintained by **Jianhao Wei**, and are released under the same license.

## 🙋 Author

**Jianhao Wei**
Software Engineering Institute, East China Normal University
Email: [w1397800@126.com]
GitHub: https://github.com/w1397800/Mypeloton/tree/8.0

If you use Mypeloton in research or applications, please cite or reference:
> Jianhao Wei. Mypeloton: An In-Memory OLTP Database Engine Based on MySQL. GitHub: https://github.com/w1397800/mypeloton
