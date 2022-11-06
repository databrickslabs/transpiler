# SIEM-to-PySpark transpiler

Cybersecurity practitioners have plenty of ETL or alerting rules coded in SIEM language to run within some of the industry-standard SIEM environments. In reality, only the most common commands are used the most by SIEM practitioners, and it’s possible to automatically translate them into corresponding PySpark Structured Streaming or, even later - Spark SQL so that we get the same results on the same datasets with the same query from both SIEM and Databricks. It’s also possible to use this tooling to teach PySpark equivalents to SIEM practitioners to accelerate their time-to-comfort level with Databricks Lakehouse foundations.

![.](spark-spl.png)

Queries could be manually translated, requiring expert knowledge of SIEM and PySpark (or SQL). It takes a week to translate a dozen queries but may take months to translate hundreds of those. With this cross-compiler, we can cut down migration time from months to weeks or even days and significantly increase the speed of learning for the new Databricks practitioners. Some tools like https://uncoder.io/ or https://github.com/SigmaHQ/sigma translate SIEM into other formats, but none exist so far that translates a vast amount of queries to PySpark or Spark SQL.

Lakehouse is especially useful for historical data analysis where only shallow history of records would be kept on SIEM-based architecture. The ability to translate SQL or Spark-based queries on entire history and combine these queries with advanced analytics capabilities offer cyber analysts a more holistic view of security anomalies, and more accurate detection of advanced persistent threats more full history is needed.

## Language support

There's basic support for the most used commands like `addtotals`, `bin`, `collect`, `convert`, `dedup`, `eval`, `eventstats`, `fields`, `fillnull`, 
`format`, `head`, `inputlookup`, `join`, `lookup`, `makeresults`, `map`, `multisearch`, 
`mvcombine`, `mvexpand`, `regex`, `rename`, `return`, `rex`, `search`, `sort`, `stats`, 
`streamstats`, `table`, `where`.

There's also basic support for functions like `auto()`, `cidr_match()`, `coalesce()`, `count()`, 
`ctime()`, `earliest()`, `if()`, `isnotnull()`, `latest()`, `len()`, `lower()`, `max()`, 
`memk()`, `min()`, `mvappend()`, `mvcount()`, `mvfilter()`, `mvindex()`, `none()`, 
`null()`, `num()`, `replace()`, `rmcomma()`, `rmunit()`, `round()`, `strftime()`, 
`substr()`, `sum()`, `term()`, `values()`. 

### Overriding `_time`, `_raw` and index names

Your Delta Lake may have other column names containing timestamps and raw records, so you can override those by 
setting the following spark conf values from code or cluster config:

```
spark.conf.set("spl.field._time", "ts")
spark.conf.set("spl.field._raw", "json")
spark.conf.set("spl.index", "custom_table")
```

## Quickstart

For code-generation mode:

```shell script
mvn -DskipTests=true -Plocal package
cat query.txt | java -jar target/transpiler-0.4.jar > pyspark-equivalent.py
```

## Featured on

* [Cutting the Edge in Fighting Cybercrime: Reverse-Engineering a Search Language to Cross-Compile to PySpark](https://www.youtube.com/watch?v=y8rKzRaM7c4) at DATA+AI Summit 2022
* [Accidentally Building a Petabyte-Scale Cybersecurity Data Mesh in Azure With Delta Lake](https://www.youtube.com/watch?v=G9x-1s-1TJI) at DATA+AI Summit 2022


### Project Support
Please note that all projects in the `databrickslabs` github space are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as [GitHub Issues](https://github.com/databrickslabs/transpiler/issues) on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.
