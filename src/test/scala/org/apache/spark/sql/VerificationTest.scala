package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite

class VerificationTest extends AnyFunSuite with ProcessProxy {

  val dummy = Seq(
    Dummy("a", "b", "c", 1, valid = true),
    Dummy("d", "e", "f", 2, valid = false),
    Dummy("g", "h", "i", 3, valid = true),
    Dummy("h", "g", "f", 4, valid = false),
    Dummy("e", "d", "c", 5, valid = true),
  )

  val dummyWithNull = Seq(
    Dummy("a", null, null, 1, valid = true),
    Dummy("d", "e", "f", 2, valid = false)
  )

  val dummySingleField = Seq(
    SingleRawField("Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: " +
                   "<MariaDubois@example.com> To: <zecora@buttercupgames.com> RID 0 - 5.4.7 - " +
                   "Delivery expired (message too old) ('000', ['timeout'])"),
    SingleRawField("Mon Mar 19 20:16:03 2018 Info: Delayed: DCID 8414309 MID 19410908 From: " +
                   "<WeiZhang@example.com> To: <mcintosh@buttercupgames.com> RID 0 - 4.3.2 - " +
                   "Not accepting messages at this time ('421', ['4.3.2 try again later'])"),
    SingleRawField("Mon Mar 19 20:16:02 2018 Info: Bounced: DCID 0 MID 19408690 From: " +
                   "<Exit_Desk@sample.net> To: <lyra@buttercupgames.com> RID 0 - 5.1.2 - " +
                   "Bad destination host ('000', ['DNS Hard Error looking (MX):  NXDomain'])"),
    SingleRawField("Mon Mar 19 20:15:53 2018 Info: Delayed: DCID 8414166 MID 19410657 From: " +
                   "<Manish_Das@example.com> To: <dash@buttercupgames.com> RID 0 - 4.3.2 - " +
                   "Not accepting messages at this time ('421', ['4.3.2 try again later'])")
  )

  test("thing") {
    generates("n>2 | stats count() by valid",
      """(spark.table('x')
        |.where("(`n` > '2')")
        |.groupBy('valid')
        |.agg(F.expr('count() AS `count`')))
        |""".stripMargin)
  }

  test("thing2") {
    import spark.implicits._
    spark.createDataset(dummy).createOrReplaceTempView("x")
    executes("n>2",
      """+---+---+---+---+-----+
        ||a  |b  |c  |n  |valid|
        |+---+---+---+---+-----+
        ||g  |h  |i  |3  |true |
        ||h  |g  |f  |4  |false|
        ||e  |d  |c  |5  |true |
        |+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\"") {
    import spark.implicits._

    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(dummySingleField).createOrReplaceTempView("x")

    executes("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\"",
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
    import spark.implicits._

    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(dummySingleField).createOrReplaceTempView("x")

    executes("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw",
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
    import spark.implicits._

    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(dummy).createOrReplaceTempView("x")

    executes("rename a as a1",
      """+---+---+---+-----+---+
        ||  b|  c|  n|valid| a1|
        |+---+---+---+-----+---+
        ||  b|  c|  1| true|  a|
        ||  e|  f|  2|false|  d|
        ||  h|  i|  3| true|  g|
        ||  g|  f|  4|false|  h|
        ||  d|  c|  5| true|  e|
        |+---+---+---+-----+---+
        |""".stripMargin, truncate = 30)
  }

  test("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw | rename from AS emailFrom, to AS emailTo") {
    import spark.implicits._

    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(dummySingleField).createOrReplaceTempView("x")

    executes("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw | rename from AS emailFrom, to AS emailTo",
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
    import spark.implicits._

    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(dummySingleField).createOrReplaceTempView("x")

    executes("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw | return 4 emailFrom=from emailTo=to",
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
    import spark.implicits._

    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(dummy).createOrReplaceTempView("x")

    executes("join type=inner a [search a=\"a\"]",
    """+---+---+---+---+-----+---+---+---+-----+
      ||a  |b  |c  |n  |valid|b  |c  |n  |valid|
      |+---+---+---+---+-----+---+---+---+-----+
      ||a  |b  |c  |1  |true |b  |c  |1  |true |
      |+---+---+---+---+-----+---+---+---+-----+
      |""".stripMargin)
  }

  test("join type=left a [search a=\"a\"]") {
    import spark.implicits._

    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(dummy).createOrReplaceTempView("x")

    executes("join type=left a [search a=\"a\"]",
      """+---+---+---+---+-----+----+----+----+-----+
        ||a  |b  |c  |n  |valid|b   |c   |n   |valid|
        |+---+---+---+---+-----+----+----+----+-----+
        ||a  |b  |c  |1  |true |b   |c   |1   |true |
        ||d  |e  |f  |2  |false|null|null|null|null |
        ||g  |h  |i  |3  |true |null|null|null|null |
        ||h  |g  |f  |4  |false|null|null|null|null |
        ||e  |d  |c  |5  |true |null|null|null|null |
        |+---+---+---+---+-----+----+----+----+-----+
        |""".stripMargin)
  }

  test("fillnull") {
    import spark.implicits._

    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(dummyWithNull).createOrReplaceTempView("x")

    executes("fillnull",
      """+---+---+---+---+-----+
        ||a  |b  |c  |n  |valid|
        |+---+---+---+---+-----+
        ||a  |0  |0  |1  |true |
        ||d  |e  |f  |2  |false|
        |+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("fillnull value=NA") {
    import spark.implicits._

    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(dummyWithNull).createOrReplaceTempView("x")

    executes("fillnull value=NA",
      """+---+---+---+---+-----+
        ||a  |b  |c  |n  |valid|
        |+---+---+---+---+-----+
        ||a  |NA |NA |1  |true |
        ||d  |e  |f  |2  |false|
        |+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("fillnull value=NA a c n valid") {
    import spark.implicits._

    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(dummyWithNull).createOrReplaceTempView("x")

    executes("fillnull value=NA a c n valid",
      """+---+----+---+---+-----+
        ||a  |b   |c  |n  |valid|
        |+---+----+---+---+-----+
        ||a  |null|NA |1  |true |
        ||d  |e   |f  |2  |false|
        |+---+----+---+---+-----+
        |""".stripMargin)
  }

}
