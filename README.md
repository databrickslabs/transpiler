# Splunk Query Language to PySpark transpiler

![.](spark-spl.png)

## Quickstart

For code-generation mode:

```shell script
mvn package
cat target/spl-query.txt | java -jar target/spark-spl-0.1-jar-with-dependencies.jar > pyspark-equivalent.py
```

## Commands

| Command/Feature | Parser | Catalyst | Codegen |
| ---: | :---: | :---: | :---: |
| search | ✅ | ✅ | ✅ |
| eval | ✅ | ✅ | ✅ |
| table | ✅ | ✅ | ✅ |
| rex | ✅ | ✅ | ✅ |
| regex | ✅ | ✅ | ✅ |
| where | ✅ | ✅ | ✅ |
| lookup | ✅ | ✅ | ✅ |
| bin | ✅ | ✅ | ✅ |
| rename | ✅ | ✅ | ✅ |
| join | ✅ | ✅ | ✅ |
| fields | ✅ | ✅ | ✅ |
| convert | ✅ |  |  |
| collect | ✅ | ✅ | ✅ |
| stats | ✅ | ✅ | ✅ |
| sort | ✅ | ✅ | ✅ |
| return | ✅ | ✅ | ✅ |
| mvcombine | ✅ | ✅ | ✅ |
| map |  |  |  |
| inputlookup | ✅ | ✅ | ✅ |
| head | ✅ | ✅ | ✅ |
| format | ✅ | ✅ | ✅ |
| fillnull | ✅ | ✅ | ✅ |
| eventstats | ✅ | ✅ | ✅ |
| dedup | ✅ | ✅ | ✅ |

Secondary batch of commands:

| Command/Feature | Parser | Catalyst | Codegen |
| ---: | :---: | :---: | :---: |
| makeresults |  |  |  |
| tstats |  |  |  |
| mvexpand |  |  |  |
| chart |  |  |  |
| timechart |  |  |  |
| export |  |  |  |
| streamstats |  |  |  |
| geom |  |  |  |
| addtotals |  |  |  |
| foreach |  |  |  |

 
## Functions

| Function | Catalyst |
| ---: | :---: |
| term() | extension ✅ |
| [CIDR search](https://docs.splunk.com/Documentation/Splunk/8.2.2/SearchReference/ConditionalFunctions#cidrmatch.28.22X.22.2CY.29) |  |  |  |
| strftime() | ✅ |
| values() | ✅ |
| latest() | ✅ |
| earliest() | ✅ |
| if() | ✅ |
| [mvcount()](https://docs.splunk.com/Documentation/SplunkCloud/8.2.2106/SearchReference/MultivalueEvalFunctions#mvcount.28MVFIELD.29) |  |
| coalesce() | ✅ |
| mvindex() |  |
| mvappend() |  |
| null() |  |
| min() | ✅ |
| round() | ✅ |
| max() | ✅ |
| substr() | ✅ |
| isnotnull() |  |
| sum() | ✅ |
| mvfilter() |  |
| len() | ✅ |
| count() | ✅ |

## Developing 

Parsers are implemented using [fastparse](https://github.com/com-lihaoyi/fastparse) (MIT)

Quick installation on Databricks: `mvn -DskipTests=true package && databricks --profile=demo fs cp target/spark-spl-0.1.jar dbfs:/tmp/spark-spl.jar --overwrite`

If you want to use `TERM()` function, you have to enable Spark extension:

```conf
spark.sql.extensions org.apache.spark.sql.SplExtension
```
