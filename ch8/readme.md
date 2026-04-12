# Chapter 8: Designing the federation layer

## What this chapter covers

* Evaluating requirements for data federation  
* Designing the federation layer components  
* Comparing Dremio and Trino for federated querying  
* Self-managed and cloud-managed federation options  
* Selecting a federation platform based on use cases

As your Apache Iceberg lakehouse takes shape, it is important to recognize that not all data will reside within Iceberg tables. Despite best efforts to centralize and standardize, some datasets will remain scattered, locked in third-party systems, legacy databases, and SaaS applications, or simply not worth the effort of extracting, transforming, and loading into your lakehouse. These realities make it essential to extend your architecture with a federation layer.

## Chapter Resources

- [code_snippets.md](./code_snippets.md)

