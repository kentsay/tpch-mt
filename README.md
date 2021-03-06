# tpch-mt
TPC-H Multi-tenant dbgen. Base on the work from [here](https://github.com/airlift/tpch)

### About tcph-mt dbgen
Comparing with the original TPC-H dbgen verison, the tpch-mt version extended with serveral MT features and is designed for Multi-Tenancy DB benchmark.

### Usage
`dbgen.java` is the entry point of tpch-mt application. The output is controlled by a combination of command line options
and environment variables. Command line options are assumed to be single letter flags preceded by a minus sign. They may be followed by an optional argument.

#### Command Line Options
```
 -h         -- display this message
 -s <arg>   -- set Scale Factor (SF) to <n> (default: 1)
 -t <arg>   -- set Number of Tenants to <n> (default: 1)
 -m <arg>   -- set distribution mode to <mode> (default: uniform, others:zipf)
 -T <arg>   -- generate tables
 ```
 Sample usage: `java dbgen -s 1 -t 10 -m uniform`.

### What will tpch-mt dbgen create?
tpch-mt dbgen will generate 6 separate ascii files under the `output` folder. This is not as same as original TPCH dbgen. We eliminate the PART and PARTSUPP table. Each file will contain pipe-delimited load data for one of the tables defined in the TPC-H database schema. The default tables will contain the load data required for a scale factor 1 database. By default the file will be created in the current directory and be named `<table>.tbl`. As an example, customer.tbl will contain the load data for the customer table.
