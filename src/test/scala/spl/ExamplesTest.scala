package spl

import org.apache.spark.sql.ProcessProxy
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

  test("stats sum test w/ wildcards w/ empty context") {
    // scalastyle:off
    generates("stats sum(*) by valid",
      """(spark.table('main')
        |# Error in stats: spl.catalyst.EmptyContextOutput: Unable to tanslate spl.ast.StatsCommand due to empty context output)
        |""".stripMargin)
    // scalastyle:on
  }

  test("stats sum test w/ wildcards w/o empty context") {
    generates("eval n=23 | stats sum(*) by valid",
      """(spark.table('main')
        |.withColumn('n', F.lit(23))
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
        |.withColumn('n_large', F.when((F.col('n') > F.lit(3)), F.lit(1)).otherwise(F.lit(0))))
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

  test("mvfiltered=mvfilter(d > 3)") {
    generates("eval mvfiltered=mvfilter(d > 3)",
      """(spark.table('main')
        |.withColumn('mvfiltered', F.filter(F.col('d'), lambda d: (d > F.lit(3)))))
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

  test("sub=substr(a, 3, 5)") {
    generates("eval sub=substr(a, 3, 5)",
      """(spark.table('main')
        |.withColumn('sub', F.substring(F.col('a'), 3, 5)))
        |""".stripMargin)
  }

  test("lenA=len(a)") {
    generates("eval lenA=len(a)",
      """(spark.table('main')
        |.withColumn('lenA', F.length(F.col('a'))))
        |""".stripMargin)
  }

  test("dedup 10 host") {
    // scalastyle:off
    generates("dedup 10 host",
      """(spark.table('main')
        |# Error in dedup: spl.catalyst.EmptyContextOutput: Unable to tanslate spl.ast.DedupCommand due to empty context output)
        |""".stripMargin)
    // scalastyle:on
  }

  test("format maxresults=10") {
    // scalastyle:off
    generates("format maxresults=10",
      """(spark.table('main')
        |# Error in format: spl.catalyst.EmptyContextOutput: Unable to tanslate spl.ast.FormatCommand due to empty context output)
        |""".stripMargin)
    // scalastyle:on
  }

  test("mvcombine host") {
    // scalastyle:off
    generates("mvcombine host",
      """(spark.table('main')
        |# Error in mvcombine: spl.catalyst.EmptyContextOutput: Unable to tanslate spl.ast.MvCombineCommand due to empty context output)
        |""".stripMargin)
    // scalastyle:on
  }

  test("makeresults count=10") {
    generates("makeresults count=10",
      """(spark.range(0, 10, 1)
        |.withColumn('_raw', F.lit(None))
        |.withColumn('_time', F.current_timestamp())
        |.withColumn('host', F.lit(None))
        |.withColumn('source', F.lit(None))
        |.withColumn('sourcetype', F.lit(None))
        |.withColumn('splunk_server', F.lit('local'))
        |.withColumn('splunk_server_group', F.lit(None))
        |.select('_time'))
        |""".stripMargin)
  }

  test("addtotals fieldname=num_total num_man num_woman") {
    // scalastyle:off
    generates("addtotals fieldname=num_total num_man num_woman",
      """(spark.table('main')
        |.withColumn('num_total', (F.when(F.col('num_woman').cast('double').isNotNull(), F.col('num_woman')).otherwise(F.lit(0.0)) + F.when(F.col('num_man').cast('double').isNotNull(), F.col('num_man')).otherwise(F.lit(0.0)))))
        |""".stripMargin)
    // scalastyle:on
  }

  test("custom configs") {
    // scalastyle:off
    spark.conf.set("spl.field._time", "ts")
    spark.conf.set("spl.field._raw", "json")
    spark.conf.set("spl.index", "custom_table")
    spark.range(10).createTempView("custom_table")
    val generatedCode = Transpiler.toPython(spark,
      "foo > 3 | join type=inner id [makeresults count=10 annotate=t]")
    readableAssert(
      """(spark.table('custom_table')
        |.where((F.col('foo') > F.lit(3)))
        |.join(spark.range(0, 10, 1)
        |.withColumn('json', F.lit(None))
        |.withColumn('ts', F.current_timestamp())
        |.withColumn('host', F.lit(None))
        |.withColumn('source', F.lit(None))
        |.withColumn('sourcetype', F.lit(None))
        |.withColumn('splunk_server', F.lit('local'))
        |.withColumn('splunk_server_group', F.lit(None))
        |.select('json', 'ts', 'host', 'source', 'sourcetype', 'splunk_server', 'splunk_server_group'),
        |['id'], 'inner'))
        |""".stripMargin, generatedCode, "Code does not match")
    spark.conf.set("spl.field._time", "_time")
    spark.conf.set("spl.field._raw", "_raw")
    spark.conf.set("spl.index", "main")
    // scalastyle:on
  }

  test("tstats sum(n) AS sumN WHERE index=main BY host, _time span=1d") {
    generates("tstats sum(n) AS sumN WHERE index=main BY host, _time span=1d",
      """(spark.table('main')
        |.withColumn('window', F.window(F.col('_time'), '24 hours'))
        |.withColumn('window', F.col('window.start'))
        |.groupBy('host', 'window')
        |.agg(F.sum(F.col('n')).alias('sumN')))
        |""".stripMargin)
  }

  test("in_range=if(cidrmatch('10.0.0.0/24', src_ip), 1, 0)") {
    // scalastyle:off
    generates("eval in_range=if(cidrmatch(\"10.0.0.0/24\", src_ip), 1, 0)",
      """(spark.table('main')
        |.withColumn('in_range', F.when(F.expr("cidr_match('10.0.0.0/24', src_ip)"), F.lit(1)).otherwise(F.lit(0))))
        |""".stripMargin)
    // scalastyle:on
  }

  test("in_range=if(cidrmatch(10.0.0.0/24, src_ip), 1, 0)") {
    // scalastyle:off
    generates("eval in_range=if(cidrmatch(10.0.0.0/24, src_ip), 1, 0)",
      """(spark.table('main')
        |.withColumn('in_range', F.when(F.expr("cidr_match('10.0.0.0/24', src_ip)"), F.lit(1)).otherwise(F.lit(0))))
        |""".stripMargin)
    // scalastyle:on
  }

  test("src_ip = 10.0.0.0/16") {
    generates("src_ip = 10.0.0.0/16",
      """(spark.table('main')
        |.where(F.expr("cidr_match('10.0.0.0/16', src_ip)")))
        |""".stripMargin)
  }

  test("fsize_quant=memk(fsize)") {
    // scalastyle:off
    generates("eval fsize_quant=memk(fsize)",
    """(spark.table('main')
      |.withColumn('fsize_quant', (F.regexp_extract(F.col('fsize'), '(?i)^(\\d*\\.?\\d+)([kmg])$', 1).cast('double') * F.when((F.upper(F.regexp_extract(F.col('fsize'), '(?i)^(\\d*\\.?\\d+)([kmg])$', 2)) == F.lit('K')), F.lit(1.0))
      |.when((F.upper(F.regexp_extract(F.col('fsize'), '(?i)^(\\d*\\.?\\d+)([kmg])$', 2)) == F.lit('M')), F.lit(1024.0))
      |.when((F.upper(F.regexp_extract(F.col('fsize'), '(?i)^(\\d*\\.?\\d+)([kmg])$', 2)) == F.lit('G')), F.lit(1048576.0))
      |.otherwise(F.lit(1.0)))))
      |""".stripMargin)
    // scalastyle:on
  }

  test("rmunit=rmunit(fsize)") {
    // scalastyle:off
    generates("eval rmunit=rmunit(fsize)",
      """(spark.table('main')
        |.withColumn('rmunit', F.regexp_extract(F.col('fsize'), '(?i)^(\\d*\\.?\\d+)(\\w*)$', 1).cast('double')))
        |""".stripMargin)
    // scalastyle:on
  }

  test("rmcomma=rmcomma(s)") {
    generates("eval rmcomma=rmcomma(s)",
      """(spark.table('main')
        |.withColumn('rmcomma', F.regexp_replace(F.col('s'), ',', '').cast('double')))
        |""".stripMargin)
  }

  test("convert timeformat=\"%Y\" ctime(_time) AS year") {
    generates("convert timeformat=\"%Y\" ctime(_time) AS year",
        """(spark.table('main')
        |.withColumn('year', F.date_format(F.col('_time'), 'yyyy')))
        |""".stripMargin)
  }

  test("convert timeformat=\"%Y\" num(_time) AS year") {
    // scalastyle:off
    generates("convert timeformat=\"%Y\" num(_time) AS year",
      """(spark.table('main')
        |.withColumn('year', F.when(F.date_format(F.col('_time').cast('string'), 'yyyy').isNotNull(), F.date_format(F.col('_time').cast('string'), 'yyyy'))
        |.when(F.col('_time').cast('double').isNotNull(), F.col('_time').cast('double'))
        |.when((F.regexp_extract(F.col('_time'), '(?i)^(\\d*\\.?\\d+)([kmg])$', 1).cast('double') * F.when((F.upper(F.regexp_extract(F.col('_time'), '(?i)^(\\d*\\.?\\d+)([kmg])$', 2)) == F.lit('K')), F.lit(1.0))
        |.when((F.upper(F.regexp_extract(F.col('_time'), '(?i)^(\\d*\\.?\\d+)([kmg])$', 2)) == F.lit('M')), F.lit(1024.0))
        |.when((F.upper(F.regexp_extract(F.col('_time'), '(?i)^(\\d*\\.?\\d+)([kmg])$', 2)) == F.lit('G')), F.lit(1048576.0))
        |.otherwise(F.lit(1.0))).isNotNull(), (F.regexp_extract(F.col('_time'), '(?i)^(\\d*\\.?\\d+)([kmg])$', 1).cast('double') * F.when((F.upper(F.regexp_extract(F.col('_time'), '(?i)^(\\d*\\.?\\d+)([kmg])$', 2)) == F.lit('K')), F.lit(1.0))
        |.when((F.upper(F.regexp_extract(F.col('_time'), '(?i)^(\\d*\\.?\\d+)([kmg])$', 2)) == F.lit('M')), F.lit(1024.0))
        |.when((F.upper(F.regexp_extract(F.col('_time'), '(?i)^(\\d*\\.?\\d+)([kmg])$', 2)) == F.lit('G')), F.lit(1048576.0))
        |.otherwise(F.lit(1.0))))
        |.when(F.regexp_extract(F.col('_time'), '(?i)^(\\d*\\.?\\d+)(\\w*)$', 1).cast('double').isNotNull(), F.regexp_extract(F.col('_time'), '(?i)^(\\d*\\.?\\d+)(\\w*)$', 1).cast('double'))
        |.when(F.regexp_replace(F.col('_time'), ',', '').cast('double').isNotNull(), F.regexp_replace(F.col('_time'), ',', '').cast('double'))
        |))
        |""".stripMargin)
    // scalastyle:on
  }

  test("multisearch in two indices") {
    // scalastyle:off
    generates("multisearch [index=regionA | fields +country, orders] [index=regionB | fields +country, orders]",
      """(spark.table('regionA')
        |.select('country', 'orders').unionByName(spark.table('regionB')
        |.select('country', 'orders'), allowMissingColumns=True))
        |""".stripMargin)
    // scalastyle:on
  }

  test("map search=\"search index=fake_for_join id=$id$\"") {
    generates("map search=\"search index=fake_for_join id=$id$\"",
      """(spark.table('fake_for_join')
        |.limit(10).alias('l')
        |.join(spark.table('main').alias('r'),
        |(F.col('l.id') == F.col('r.id')),
        |'left_semi'))
        |""".stripMargin)
  }
}
