# Chapter 6: Architecting the ingestion layer

## What this chapter covers

* Requirements for ingestion performance, reliability, and latency   
* Comparing batch, micro-batch, and streaming ingestion strategies  
* How Iceberg handles data writes, commits, and conflict resolution  
* Ingestion technologies such as Spark, Flink, and Others  
* Ingestion patterns for schema evolution, data quality, and auditability

The ingestion layer is the starting point of your Apache Iceberg lakehouse in practice. It is where raw data enters the system, whether from operational databases, message queues, cloud services, or external vendors. While the storage layer determines how data is preserved, the ingestion layer determines how data arrives—how fast, how clean, and how reliably.

## Chapter Resources

- [code_snippets.md](./code_snippets.md)

