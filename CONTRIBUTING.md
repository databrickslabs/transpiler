# Developing

Parsers are implemented using [fastparse](https://github.com/com-lihaoyi/fastparse) (MIT)

Quick installation on Databricks: `mvn -DskipTests=true package && databricks --profile=demo fs cp target/transpiler-0.4.jar dbfs:/tmp/transpiler.jar --overwrite`

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