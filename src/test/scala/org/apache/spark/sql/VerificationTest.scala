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
        |.where('(`n` > '2')')
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

}
