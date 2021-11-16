package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite

class ExamplesTest extends AnyFunSuite with ProcessProxy {
  test("thing") {
    generates("n>2 | stats count() by valid",
      """(spark.table('main')
        |.where((F.col('n') > F.lit(2)))
        |.groupBy('valid')
        |.agg(F.count(F.lit(1)).alias('count')))
        |""".stripMargin)
  }

  test("stats sum test w/ groupBy") {
    generates("n>2 | stats sum(n) by valid",
      """(spark.table('main')
        |.where((F.col('n') > F.lit(2)))
        |.groupBy('valid')
        |.agg(F.sum(F.col('n')).alias('sum')))
        |""".stripMargin)
  }

  test("stats sum test w/o groupBy") {
    generates("n>2 | stats sum(n)",
      """(spark.table('main')
        |.where((F.col('n') > F.lit(2)))
        |.groupBy()
        |.agg(F.sum(F.col('n')).alias('sum')))
        |""".stripMargin)
  }

  test("stats sum test w/o groupBy, w/ AS stmt") {
    generates("n>2 | stats sum(n) AS total_sum",
      """(spark.table('main')
        |.where((F.col('n') > F.lit(2)))
        |.groupBy()
        |.agg(F.sum(F.col('n')).alias('total_sum')))
        |""".stripMargin)
  }

  test("stats values(d) as set") {
    generates("stats values(d) as set",
      """(spark.table('main')
        |.groupBy()
        |.agg(F.collect_set(F.col('d')).alias('set')))
        |""".stripMargin)
  }

  test("stats latest(d) as latest") {
    generates("stats latest(d) as latest",
      """(spark.table('main')
        |.orderBy(F.col('_time').asc())
        |.groupBy()
        |.agg(F.last(F.col('d'), True).alias('latest')))
        |""".stripMargin)
  }

  test("stats earliest(d) as earliest") {
    generates("stats earliest(d) as earliest",
      """(spark.table('main')
        |.orderBy(F.col('_time').asc())
        |.groupBy()
        |.agg(F.first(F.col('d'), True).alias('earliest')))
        |""".stripMargin)
  }

  test("eval n_large=if(n > 3, 1, 0)") {
    generates("eval n_large=if(n > 3, 1, 0)",
      """(spark.table('main')
        |.withColumn('n_large', F.when((F.col('n') > F.lit(3)), F.lit(1)).otherwise(F.lit(0)))
        |""".stripMargin)
  }

  test("eval coalesced=coalesce(b,c)") {
    generates("index=main | eval coalesced=coalesce(b,c)",
      """(spark.table('main')
        |.withColumn('coalesced', F.expr('coalesce(`b`, `c`)')))
        |""".stripMargin)
  }

  test("bin span") {
    generates("bin span=5m n",
      """(spark.table('main')
        |.withColumn('n', F.window(F.col('n'), '5 minutes'))
        |.withColumn('n', F.col('n.start')))
        |""".stripMargin)
  }

  test("eval count=mvcount(d)") {
    generates("eval count=mvcount(d)",
      """(spark.table('main')
        |.withColumn('count', F.size(F.col('d'))))
        |""".stripMargin)
  }

  test("eval mvsubset=mvindex(d,0,1)") {
    generates("eval count=mvindex(d,0,1)",
      """(spark.table('main')
        |.withColumn('count', F.expr('slice(`d`, 1, 2)')))
        |""".stripMargin)
  }

  test("eval mvappended=mvappend(d,d)") {
    generates("eval mvappended=mvappend(d,d)",
      """(spark.table('main')
        |.withColumn('mvappended', F.concat(F.col('d'), F.col('d'))))
        |""".stripMargin)
  }

  test("count=mvcount(d)") {
    generates("eval count=mvcount(d)",
      """(spark.table('main')
        |.withColumn('count', F.size(F.col('d'))))
        |""".stripMargin)
  }

  test("date=strftime(_time, \"%Y-%m-%d %T\")") {
    generates("eval date=strftime(_time, \"%Y-%m-%d %T\")",
      """(spark.table('main')
        |.withColumn('date', F.date_format(F.col('_time'), 'yyyy-MM-dd HH:mm:ss')))
        |""".stripMargin)
  }

  test("min=min(n, 100)") {
    generates("eval min=min(n, 100)",
      """(spark.table('main')
        |.withColumn('min', F.least(F.col('n'), F.lit(100))))
        |""".stripMargin)
  }

  test("max=max(n, 0)") {
    generates("eval max=max(n, 0)",
      """(spark.table('main')
        |.withColumn('max', F.greatest(F.col('n'), F.lit(0))))
        |""".stripMargin)
  }

  test("rounded=round(42.003, 0)") {
    generates("eval rounded=round(42.003, 0)",
      """(spark.table('main')
        |.withColumn('rounded', F.round(F.lit(42.003), 0)))
        |""".stripMargin)
  }
}
