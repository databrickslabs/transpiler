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

## Developing 

Parsers are implemented using [fastparse](https://github.com/com-lihaoyi/fastparse) (MIT)

Quick installation on Databricks: `mvn -DskipTests=true package && databricks --profile=demo fs cp target/spark-spl-0.4.jar dbfs:/tmp/spark-spl.jar --overwrite`

If you want to use `TERM()` function, you have to enable Spark extension:

```conf
spark.sql.extensions org.apache.spark.sql.SplExtension
```

`mvn compile` should give no warnings

Scalastyle complaints:

* run `mvn scalastyle:check`
* `illegal start of simple expression` - open file with `vim` and `:goto <number>`

Code coverage report:

* `open target/site/jacoco/index.html`
