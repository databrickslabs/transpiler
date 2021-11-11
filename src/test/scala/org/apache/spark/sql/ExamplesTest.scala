package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite

class ExamplesTest extends AnyFunSuite with ProcessProxy {
  test("thing") {
    generates("n>2 | stats count() by valid",
      """(spark.table('main')
        |.where('(`n` > 2)')
        |.groupBy('valid')
        |.agg(F.count(F.lit(1)).alias('count')))
        |""".stripMargin)
  }

  test("stats sum test w/ groupBy") {
    generates("n>2 | stats sum(n) by valid",
      """(spark.table('main')
        |.where('(`n` > 2)')
        |.groupBy('valid')
        |.agg(F.sum(F.col('n')).alias('sum')))
        |""".stripMargin)
  }

  test("stats sum test w/o groupBy") {
    generates("n>2 | stats sum(n)",
      """(spark.table('main')
        |.where('(`n` > 2)')
        |.groupBy()
        |.agg(F.sum(F.col('n')).alias('sum')))
        |""".stripMargin)
  }

  test("stats sum test w/o groupBy, w/ AS stmt") {
    generates("n>2 | stats sum(n) AS total_sum",
      """(spark.table('main')
        |.where('(`n` > 2)')
        |.groupBy()
        |.agg(F.sum(F.col('n')).alias('total_sum')))
        |""".stripMargin)
  }

  test("eval n_large=if(n > 3, 1, 0)") {
    generates("eval n_large=if(n > 3, 1, 0)",
      """(spark.table('main')
        |.withColumn('n_large', F.expr('(IF((`n` > 3), 1, 0))')))
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
        |.withColumn('count', F.expr('size(`d`)')))
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
        |.withColumn('count', F.expr('size(`d`)')))
        |""".stripMargin)
  }
}
