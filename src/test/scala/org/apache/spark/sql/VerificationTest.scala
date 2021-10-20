package org.apache.spark.sql

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class VerificationTest extends AnyFunSuite with ProcessProxy with BeforeAndAfterAll {


  val dummy = Seq(
    Dummy("dummy", "a", "b", "c", 1, valid = true),
    Dummy("dummy", "d", "e", "f", 2, valid = false),
    Dummy("dummy", "g", "h", "i", 3, valid = true),
    Dummy("dummy", "h", "g", "f", 4, valid = false),
    Dummy("dummy", "e", "d", "c", 5, valid = true),
  )

  val dummyWithNull = Seq(
    Dummy("dummy_with_null", "a", null, null, 1, valid = true),
    Dummy("dummy_with_null","d", "e", "f", 2, valid = false)
  )

  val dummyWithDuplicates = Seq(
    Dummy("dummy_with_duplicates", "a", "b", "c", 1, valid = true),
    Dummy("dummy_with_duplicates", "b", "b", "c", 2, valid = true),
    Dummy("dummy_with_duplicates", "c", "b", "c", 3, valid = true),
    Dummy("dummy_with_duplicates", "d", "e", "f", 4, valid = false),
    Dummy("dummy_with_duplicates", "e", "e", "f", 5, valid = true),
  )

  val dummySingleField = Seq(
    SingleRawField("dummy_single_field", "Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: " +
                                         "<MariaDubois@example.com> To: <zecora@buttercupgames.com> RID 0 - 5.4.7 - " +
                                         "Delivery expired (message too old) ('000', ['timeout'])"),
    SingleRawField("dummy_single_field", "Mon Mar 19 20:16:03 2018 Info: Delayed: DCID 8414309 MID 19410908 From: " +
                                         "<WeiZhang@example.com> To: <mcintosh@buttercupgames.com> RID 0 - 4.3.2 - " +
                                         "Not accepting messages at this time ('421', ['4.3.2 try again later'])"),
    SingleRawField("dummy_single_field", "Mon Mar 19 20:16:02 2018 Info: Bounced: DCID 0 MID 19408690 From: " +
                                         "<Exit_Desk@sample.net> To: <lyra@buttercupgames.com> RID 0 - 5.1.2 - " +
                                         "Bad destination host ('000', ['DNS Hard Error looking (MX):  NXDomain'])"),
    SingleRawField("dummy_single_field", "Mon Mar 19 20:15:53 2018 Info: Delayed: DCID 8414166 MID 19410657 From: " +
                                         "<Manish_Das@example.com> To: <dash@buttercupgames.com> RID 0 - 4.3.2 - " +
                                         "Not accepting messages at this time ('421', ['4.3.2 try again later'])")
  )

  override def beforeAll(): Unit = {
    import spark.implicits._
    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(dummy).createOrReplaceTempView("dummy")
    spark.createDataset(dummySingleField).createOrReplaceTempView("dummy_single_field")
    spark.createDataset(dummyWithNull).createOrReplaceTempView("dummy_with_null")
    spark.createDataset(dummyWithDuplicates).createOrReplaceTempView("dummy_with_duplicates")
  }

  test("thing") {
    generates("n>2 | stats count() by valid",
      """(spark.table('main')
        |.where('(`n` > 2)')
        |.groupBy('valid')
        |.agg(F.expr('count() AS `count`')))
        |""".stripMargin)
  }

  test("thing2") {
    executes("index=dummy | n>2",
      """+-----+---+---+---+---+-----+
        ||index|a  |b  |c  |n  |valid|
        |+-----+---+---+---+---+-----+
        ||dummy|g  |h  |i  |3  |true |
        ||dummy|h  |g  |f  |4  |false|
        ||dummy|e  |d  |c  |5  |true |
        |+-----+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\"") {
    executes("index=dummy_single_field | rex \"From: <(?<from>.*)> To: <(?<to>.*)>\"",
      """+------------------------------+-----------------------+---------------------------+
        ||                          _raw|                   from|                         to|
        |+------------------------------+-----------------------+---------------------------+
        ||Mon Mar 19 20:16:27 2018 In...|MariaDubois@example.com|  zecora@buttercupgames.com|
        ||Mon Mar 19 20:16:03 2018 In...|   WeiZhang@example.com|mcintosh@buttercupgames.com|
        ||Mon Mar 19 20:16:02 2018 In...|   Exit_Desk@sample.net|    lyra@buttercupgames.com|
        ||Mon Mar 19 20:15:53 2018 In...| Manish_Das@example.com|    dash@buttercupgames.com|
        |+------------------------------+-----------------------+---------------------------+
        |""".stripMargin, truncate = 30)
  }

  test("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw") {
    executes("index=dummy_single_field | rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw",
      """+-----------------------+---------------------------+
        ||                   from|                         to|
        |+-----------------------+---------------------------+
        ||MariaDubois@example.com|  zecora@buttercupgames.com|
        ||   WeiZhang@example.com|mcintosh@buttercupgames.com|
        ||   Exit_Desk@sample.net|    lyra@buttercupgames.com|
        || Manish_Das@example.com|    dash@buttercupgames.com|
        |+-----------------------+---------------------------+
        |""".stripMargin, truncate = 30)
  }

  test("rename a as a1") {
    executes("index=dummy | rename a as a1",
      """+-----+---+---+---+-----+---+
        ||index|  b|  c|  n|valid| a1|
        |+-----+---+---+---+-----+---+
        ||dummy|  b|  c|  1| true|  a|
        ||dummy|  e|  f|  2|false|  d|
        ||dummy|  h|  i|  3| true|  g|
        ||dummy|  g|  f|  4|false|  h|
        ||dummy|  d|  c|  5| true|  e|
        |+-----+---+---+---+-----+---+
        |""".stripMargin, truncate = 30)
  }

  test("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw | rename from AS emailFrom, to AS emailTo") {
    executes("index=dummy_single_field | rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw | rename from AS emailFrom, to AS emailTo",
      """+-----------------------+---------------------------+
        ||              emailFrom|                    emailTo|
        |+-----------------------+---------------------------+
        ||MariaDubois@example.com|  zecora@buttercupgames.com|
        ||   WeiZhang@example.com|mcintosh@buttercupgames.com|
        ||   Exit_Desk@sample.net|    lyra@buttercupgames.com|
        || Manish_Das@example.com|    dash@buttercupgames.com|
        |+-----------------------+---------------------------+
        |""".stripMargin, truncate = 30)
  }

  test("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw | return 4 emailFrom=from emailTo=to") {
    executes("index=dummy_single_field | rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw | return 4 emailFrom=from emailTo=to",
      """+-----------------------+---------------------------+
        ||              emailFrom|                    emailTo|
        |+-----------------------+---------------------------+
        ||MariaDubois@example.com|  zecora@buttercupgames.com|
        ||   WeiZhang@example.com|mcintosh@buttercupgames.com|
        ||   Exit_Desk@sample.net|    lyra@buttercupgames.com|
        || Manish_Das@example.com|    dash@buttercupgames.com|
        |+-----------------------+---------------------------+
        |""".stripMargin, truncate = 30)
  }

  test("join type=inner a [search a=\"a\"]") {
    executes("index=dummy | join type=inner a [search index=dummy a=\"a\"]",
    """+---+-----+---+---+---+-----+-----+---+---+---+-----+
      ||a  |index|b  |c  |n  |valid|index|b  |c  |n  |valid|
      |+---+-----+---+---+---+-----+-----+---+---+---+-----+
      ||a  |dummy|b  |c  |1  |true |dummy|b  |c  |1  |true |
      |+---+-----+---+---+---+-----+-----+---+---+---+-----+
      |""".stripMargin)
  }

  test("join type=left a [search a=\"a\"]") {
    executes("index=dummy | join type=left a [search index=dummy a=\"a\"]",
      """+---+-----+---+---+---+-----+-----+----+----+----+-----+
        ||a  |index|b  |c  |n  |valid|index|b   |c   |n   |valid|
        |+---+-----+---+---+---+-----+-----+----+----+----+-----+
        ||a  |dummy|b  |c  |1  |true |dummy|b   |c   |1   |true |
        ||d  |dummy|e  |f  |2  |false|null |null|null|null|null |
        ||g  |dummy|h  |i  |3  |true |null |null|null|null|null |
        ||h  |dummy|g  |f  |4  |false|null |null|null|null|null |
        ||e  |dummy|d  |c  |5  |true |null |null|null|null|null |
        |+---+-----+---+---+---+-----+-----+----+----+----+-----+
        |""".stripMargin)
  }

  test("fillnull") {
    executes("index=dummy_with_null | fillnull",
      """+---------------+---+---+---+---+-----+
        ||index          |a  |b  |c  |n  |valid|
        |+---------------+---+---+---+---+-----+
        ||dummy_with_null|a  |0  |0  |1  |true |
        ||dummy_with_null|d  |e  |f  |2  |false|
        |+---------------+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("fillnull value=NA") {
    executes("index=dummy_with_null | fillnull value=NA",
      """+---------------+---+---+---+---+-----+
        ||index          |a  |b  |c  |n  |valid|
        |+---------------+---+---+---+---+-----+
        ||dummy_with_null|a  |NA |NA |1  |true |
        ||dummy_with_null|d  |e  |f  |2  |false|
        |+---------------+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("fillnull value=NA a c n valid") {
    executes("index=dummy_with_null | fillnull value=NA a c n valid",
      """+---------------+---+----+---+---+-----+
        ||index          |a  |b   |c  |n  |valid|
        |+---------------+---+----+---+---+-----+
        ||dummy_with_null|a  |null|NA |1  |true |
        ||dummy_with_null|d  |e   |f  |2  |false|
        |+---------------+---+----+---+---+-----+
        |""".stripMargin)
  }

  test("dedup 1 b c") {
    executes("index=dummy_with_duplicates | dedup 1 b c",
      """+---------------------+---+---+---+---+-----+
        ||index                |a  |b  |c  |n  |valid|
        |+---------------------+---+---+---+---+-----+
        ||dummy_with_duplicates|d  |e  |f  |4  |false|
        ||dummy_with_duplicates|a  |b  |c  |1  |true |
        |+---------------------+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("dedup b c sortby -n") {
    executes("index=dummy_with_duplicates | dedup b c sortby -n",
      """+---------------------+---+---+---+---+-----+
        ||index                |a  |b  |c  |n  |valid|
        |+---------------------+---+---+---+---+-----+
        ||dummy_with_duplicates|e  |e  |f  |5  |true |
        ||dummy_with_duplicates|c  |b  |c  |3  |true |
        |+---------------------+---+---+---+---+-----+
        |""".stripMargin)
  }
}
