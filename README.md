# SPL-to-PySpark transpiler

![.](spark-spl.png)

## Quickstart

For code-generation mode:

```shell script
mvn -DskipTests=true -Plocal package
cat spl-query.txt | java -jar target/spark-spl-0.4.jar > pyspark-equivalent.py
```

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

## Featured on

* [Cutting the Edge in Fighting Cybercrime: Reverse-Engineering a Search Language to Cross-Compile to PySpark](https://www.youtube.com/watch?v=y8rKzRaM7c4) at DATA+AI Summit 2022
* [Accidentally Building a Petabyte-Scale Cybersecurity Data Mesh in Azure With Delta Lake](https://www.youtube.com/watch?v=G9x-1s-1TJI) at DATA+AI Summit 2022


### Project Support
Please note that all projects in the `databrickslabs` github space are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.
