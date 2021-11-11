package org.apache.spark.sql

import java.sql.Timestamp

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class VerificationTest extends AnyFunSuite with ProcessProxy with BeforeAndAfterAll {

  val dummy = Seq(
    Dummy("a", "b", "c", 1, valid = true),
    Dummy("d", "e", "f", 2, valid = false),
    Dummy("g", "h", "i", 3, valid = true),
    Dummy("h", "g", "f", 4, valid = false),
    Dummy("e", "d", "c", 5, valid = true),
  )

  val dummyWithArray = Seq(
    DummyWithArray("a", "b", "c", Seq("a", "b", "cde"), 1, valid = true),
    DummyWithArray("d", "e", "f", Seq("d", "e"), 2, valid = false),
    DummyWithArray("g", "h", "i", Seq("g", "h", "i"), 3, valid = true),
    DummyWithArray("h", "g", "f", Seq("h"), 4, valid = false),
    DummyWithArray("e", "d", "c", Seq("e", "d", "c"), 5, valid = true),
  )

  val dummyWithIntArray = Seq(
    DummyWithIntArray("a", "b", "c", Seq(1, 2, 3), 1, valid = true),
    DummyWithIntArray("d", "e", "f", Seq(4, 5), 2, valid = false),
    DummyWithIntArray("g", "h", "i", Seq(6, 7, 8), 3, valid = true),
    DummyWithIntArray("h", "g", "f", Seq(9), 4, valid = false),
    DummyWithIntArray("e", "d", "c", Seq(5, 4, 3), 5, valid = true),
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

  val dummySubstrings = Seq(
    Dummy("a_abc", "abc", "a_ghi", 1, valid = true),
    Dummy("a_abc_d", "abc", "a_ghi", 2, valid = false)
  )

  val dummyWithDuplicates = Seq(
    Dummy("a", "b", "c", 1, valid = true),
    Dummy("b", "b", "c", 2, valid = true),
    Dummy("c", "b", "c", 3, valid = true),
    Dummy("d", "e", "f", 4, valid = false),
    Dummy("e", "e", "f", 5, valid = true),
  )

  val countryByContinent = Seq(
    CountryByContinent("Europe", "Albania"),
    CountryByContinent("Europe", "Bulgaria"),
    CountryByContinent("Europe", "Belarus"),
    CountryByContinent("Africa", "Congo"),
    CountryByContinent("Africa", "Egypt"),
    CountryByContinent("Africa", "Algeria")
  )

  val dummyFlow = Seq(
    Flow(Timestamp.valueOf("2021-11-04 09:12:34"), "10.1.1.2", "178.0.3.56", "http"),
    Flow(Timestamp.valueOf("2021-11-04 09:13:04"), "10.1.1.2", "10.1.2.25", "ftp"),
    Flow(Timestamp.valueOf("2021-11-04 09:16:31"), "10.1.2.25", "10.4.15.7", "ssh"),
    Flow(Timestamp.valueOf("2021-11-04 12:14:01"), "10.1.1.2", "178.0.3.56", "ssh"),
    Flow(Timestamp.valueOf("2021-11-04 12:14:02"), "178.0.3.56", "10.1.2.25", "ssh"),
    Flow(Timestamp.valueOf("2021-11-05 03:43:54"), "178.45.15.10", "10.1.2.25", "ssh"),
  )

  override def beforeAll(): Unit = {
    import spark.implicits._
    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(dummy).createOrReplaceTempView("dummy")
    spark.createDataset(dummyFlow).createOrReplaceTempView("flow")
    spark.createDataset(dummySingleField).createOrReplaceTempView("dummy_single_field")
    spark.createDataset(dummyWithNull).createOrReplaceTempView("dummy_with_null")
    spark.createDataset(dummySubstrings).createOrReplaceTempView("dummy_substrings")
    spark.createDataset(dummyWithDuplicates).createOrReplaceTempView("dummy_with_duplicates")
    spark.createDataset(countryByContinent).createOrReplaceTempView("countries")
  }

  test("bin span execute") {
    executes("index=flow | bin span=5m ts | stats count by ts | sort ts",
      """+-------------------+-----+
        ||ts                 |count|
        |+-------------------+-----+
        ||2021-11-04 09:10:00|2    |
        ||2021-11-04 09:15:00|1    |
        ||2021-11-04 12:10:00|2    |
        ||2021-11-05 03:40:00|1    |
        |+-------------------+-----+
        |""".stripMargin)
  }

  test("thing2") {
    executes("index=dummy | n>2",
      """+---+---+---+---+-----+
        ||a  |b  |c  |n  |valid|
        |+---+---+---+---+-----+
        ||g  |h  |i  |3  |true |
        ||h  |g  |f  |4  |false|
        ||e  |d  |c  |5  |true |
        |+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("mvfilter(d > 3)") {
    import spark.implicits._
    spark.createDataset(dummyWithIntArray).createOrReplaceTempView("main")
    executes("n > mvcount(mvfilter(d > 3))",
      """+---+---+---+---------+---+-----+
        ||a  |b  |c  |d        |n  |valid|
        |+---+---+---+---------+---+-----+
        ||a  |b  |c  |[1, 2, 3]|1  |true |
        ||h  |g  |f  |[9]      |4  |false|
        ||e  |d  |c  |[5, 4, 3]|5  |true |
        |+---+---+---+---------+---+-----+
        |""".stripMargin)
  }

  test("mvfilter(len(d) > 1)") {
    import spark.implicits._
    spark.createDataset(dummyWithArray).createOrReplaceTempView("main")
    executes("mvcount(mvfilter(len(d) > 1)) = 1",
      """+---+---+---+-----------+---+-----+
        ||a  |b  |c  |d          |n  |valid|
        |+---+---+---+-----------+---+-----+
        ||a  |b  |c  |[a, b, cde]|1  |true |
        |+---+---+---+-----------+---+-----+
        |""".stripMargin)
  }

  test("n > len(a)") {
    executes("index=dummy | n > len(a)",
      """+---+---+---+---+-----+
        ||a  |b  |c  |n  |valid|
        |+---+---+---+---+-----+
        ||d  |e  |f  |2  |false|
        ||g  |h  |i  |3  |true |
        ||h  |g  |f  |4  |false|
        ||e  |d  |c  |5  |true |
        |+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("b = substr(a,3)") {
    executes("index=dummy_substrings | b = substr(a,3)",
      """+-----+---+-----+---+-----+
        ||a    |b  |c    |n  |valid|
        |+-----+---+-----+---+-----+
        ||a_abc|abc|a_ghi|1  |true |
        |+-----+---+-----+---+-----+
        |""".stripMargin)
  }

  test("b = substr(a,3,3)") {
    executes("index=dummy_substrings | b = substr(a,3,3)",
      """+-------+---+-----+---+-----+
        ||a      |b  |c    |n  |valid|
        |+-------+---+-----+---+-----+
        ||a_abc  |abc|a_ghi|1  |true |
        ||a_abc_d|abc|a_ghi|2  |false|
        |+-------+---+-----+---+-----+
        |""".stripMargin)
  }

  test("b = substr(a,-3)") {
    executes("index=dummy_substrings | b = substr(a,-3)",
      """+-----+---+-----+---+-----+
        ||a    |b  |c    |n  |valid|
        |+-----+---+-----+---+-----+
        ||a_abc|abc|a_ghi|1  |true |
        |+-----+---+-----+---+-----+
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
    executes("index=dummy | rename a as a1 | rename a1 as a | rename a as a1",
      """+---+---+---+---+-----+
        || a1|  b|  c|  n|valid|
        |+---+---+---+---+-----+
        ||  a|  b|  c|  1| true|
        ||  d|  e|  f|  2|false|
        ||  g|  h|  i|  3| true|
        ||  h|  g|  f|  4|false|
        ||  e|  d|  c|  5| true|
        |+---+---+---+---+-----+
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
    """+---+---+---+---+-----+---+---+---+-----+
      ||a  |b  |c  |n  |valid|b  |c  |n  |valid|
      |+---+---+---+---+-----+---+---+---+-----+
      ||a  |b  |c  |1  |true |b  |c  |1  |true |
      |+---+---+---+---+-----+---+---+---+-----+
      |""".stripMargin)
  }

  test("join type=left a [search a=\"a\"]") {
    executes("index=dummy | join type=left a [search index=dummy a=\"a\"]",
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
    executes("index=dummy_with_null | fillnull",
      """+---+---+---+---+-----+
        ||a  |b  |c  |n  |valid|
        |+---+---+---+---+-----+
        ||a  |0  |0  |1  |true |
        ||d  |e  |f  |2  |false|
        |+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("fillnull value=NA") {
    executes("index=dummy_with_null | fillnull value=NA",
      """+---+---+---+---+-----+
        ||a  |b  |c  |n  |valid|
        |+---+---+---+---+-----+
        ||a  |NA |NA |1  |true |
        ||d  |e  |f  |2  |false|
        |+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("fillnull value=NA a c n valid") {
    executes("index=dummy_with_null | fillnull value=NA a c n valid",
      """+---+----+---+---+-----+
        ||a  |b   |c  |n  |valid|
        |+---+----+---+---+-----+
        ||a  |null|NA |1  |true |
        ||d  |e   |f  |2  |false|
        |+---+----+---+---+-----+
        |""".stripMargin)
  }

  test("eventstats max(n) AS max_n, min(n) by b") {
    executes("index=dummy_with_duplicates | eventstats max(n) AS max_n, min(n) by b",
      """+---+---+---+---+-----+-----+------+
        ||a  |b  |c  |n  |valid|max_n|min(n)|
        |+---+---+---+---+-----+-----+------+
        ||d  |e  |f  |4  |false|5    |4     |
        ||e  |e  |f  |5  |true |5    |4     |
        ||a  |b  |c  |1  |true |3    |1     |
        ||b  |b  |c  |2  |true |3    |1     |
        ||c  |b  |c  |3  |true |3    |1     |
        |+---+---+---+---+-----+-----+------+
        |""".stripMargin)
  }

  test("dedup 1 b c") {
    executes("index=dummy_with_duplicates | dedup 1 b c",
      """+---+---+---+---+-----+
        ||a  |b  |c  |n  |valid|
        |+---+---+---+---+-----+
        ||d  |e  |f  |4  |false|
        ||a  |b  |c  |1  |true |
        |+---+---+---+---+-----+
        |""".stripMargin)
  }

  /**
   * Dummy("a", "b", "c", 1, valid = true),
   * Dummy("d", "e", "f", 2, valid = false),
   */
  test("inputlookup dummy where n < 3") {
    executes("inputlookup dummy where n < 3",
      """+---+---+---+---+-----+
        ||a  |b  |c  |n  |valid|
        |+---+---+---+---+-----+
        ||a  |b  |c  |1  |true |
        ||d  |e  |f  |2  |false|
        |+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("inputlookup max=2 dummy where n < 10") {
    executes("inputlookup dummy where n < 3",
      """+---+---+---+---+-----+
        ||a  |b  |c  |n  |valid|
        |+---+---+---+---+-----+
        ||a  |b  |c  |1  |true |
        ||d  |e  |f  |2  |false|
        |+---+---+---+---+-----+
        |""".stripMargin)
  }

  test("format maxresults=2") {
    executes("index=dummy | format maxresults=2",
      """+-----------------------------------------------------------------------------------------------------------------+
        ||search                                                                                                           |
        |+-----------------------------------------------------------------------------------------------------------------+
        ||((a=a) AND (b=b) AND (c=c) AND (n=1) AND (valid=true)) OR ((a=d) AND (b=e) AND (c=f) AND (n=2) AND (valid=false))|
        |+-----------------------------------------------------------------------------------------------------------------+
        |""".stripMargin)
  }

  test("format maxresults=2 \"[\" \"[\" \"&&\" \"]\" \"||\" \"]\"") {
    executes("index=dummy | format maxresults=2 \"[\" \"[\" \"&&\" \"]\" \"||\" \"]\"",
      """+---------------------------------------------------------------------------------------------------------+
        ||search                                                                                                   |
        |+---------------------------------------------------------------------------------------------------------+
        ||[[a=a] && [b=b] && [c=c] && [n=1] && [valid=true]] || [[a=d] && [b=e] && [c=f] && [n=2] && [valid=false]]|
        |+---------------------------------------------------------------------------------------------------------+
        |""".stripMargin)
  }

  test("mvcombine country") {
    executes("index=countries | mvcombine country",
    """+---------+----------------------------+
      ||continent|country                     |
      |+---------+----------------------------+
      ||Europe   |[Albania, Bulgaria, Belarus]|
      ||Africa   |[Congo, Egypt, Algeria]     |
      |+---------+----------------------------+
      |""".stripMargin)
  }

  test("mvcombine delim=\";\" country") {
    executes("index=countries | mvcombine delim=\";\" country",
      """+---------+------------------------+
        ||continent|country                 |
        |+---------+------------------------+
        ||Europe   |Albania;Bulgaria;Belarus|
        ||Africa   |Congo;Egypt;Algeria     |
        |+---------+------------------------+
        |""".stripMargin)
  }
}
