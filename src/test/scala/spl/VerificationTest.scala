package spl

import java.sql.Timestamp

import org.apache.spark.sql.{FakeData, FakeDataForJoin, ProcessProxy}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class VerificationTest extends AnyFunSuite with ProcessProxy with BeforeAndAfterAll {

  // scalastyle:off
  val fakeData = Seq(
    FakeData(1, "M", "jcraisford0@imdb.com", "109.177.141.88", Seq("G+", "F+"), "China", "maestro", 6304276470412087L, true, Timestamp.valueOf("2021-11-05 21:20:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <twraggsx@phpbb.com> To: <vstovin2l@tripadvisor.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(2, "M", null, null, Seq("C+"), "China", "jcb", 3538391327116529L, false, Timestamp.valueOf("2021-11-05 21:21:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <kquipp1q@newyorker.com> To: <kannakin13@ameblo.jp> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(3, "M", "slockyer2@fotki.com", "165.53.105.69", Seq("A+", "A", "B", "B+"), "China", "jcb", 3587986898129650L, true, Timestamp.valueOf("2021-11-05 21:22:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <tkment1h@usda.gov> To: <rdeville2e@google.de> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(4, "F", null, "156.148.239.162", Seq("A+", "A+", "C+", "G+"), "Serbia", "jcb", 3553492095595066L, true, Timestamp.valueOf("2021-11-05 21:23:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <emaccard1t@rambler.ru> To: <cyukhtinb@senate.gov> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(5, "F", "abasilotta4@mediafire.com", "65.218.253.251", Seq("A+", "C+"), "Philippines", "bankcard", 5602210998120584L, false, Timestamp.valueOf("2021-11-05 21:24:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <dswedeland1o@simplemachines.org> To: <vstovin2l@tripadvisor.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(6, "F", "bhaskins5@w3.org", "82.24.35.79", Seq("D+", "D+"), "Cameroon", "china-unionpay", 56022420047084831L, true, Timestamp.valueOf("2021-11-05 21:25:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <rjeaycocke@lulu.com> To: <jtipler1f@alibaba.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(7, "M", "arootham6@harvard.edu", "95.58.22.253", Seq("D+", "C", "G+"), "Russia", "maestro", 6759229762621737928L, false, Timestamp.valueOf("2021-11-05 21:26:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <rbaline1b@sina.com.cn> To: <tbernlin2f@bandcamp.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(8, "F", "cmacentee7@mayoclinic.com", null, Seq("C+", "C+"), "Vietnam", "maestro", 676148588124846639L, true, Timestamp.valueOf("2021-11-05 21:27:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <mbedle6@elpais.com> To: <jtipler1f@alibaba.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(9, "F", "wgasnoll8@mit.edu", null, Seq("G+", "D+"), "Estonia", "maestro", 6759854838526739L, false, Timestamp.valueOf("2021-11-05 21:28:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <bfullilove16@timesonline.co.uk> To: <cyukhtinb@senate.gov> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(10, "M", "nhartnup9@opensource.org", "4.196.92.130", Seq("G", "C+", "D", "D+"), "China", "jcb", 3538614802025554L, false, Timestamp.valueOf("2021-11-05 21:29:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <cebbetts2r@1688.com> To: <hmcgeaney1i@elpais.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(11, "M", "sflewetta@linkedin.com", "129.89.191.159", Seq("D+", "C+", "D+", "A+"), "Tunisia", "jcb", 3566491712437764L, false, Timestamp.valueOf("2021-11-05 21:30:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <mmacelroy18@shop-pro.jp> To: <ewatton26@buzzfeed.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(12, "M", "gstureb@nsw.gov.au", "108.79.235.21", Seq("D+", "D", "A", "A+"), "Russia", "solo", 676764731502161253L, false, Timestamp.valueOf("2021-11-05 21:31:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <aiwanowicz1g@yahoo.com> To: <dswedeland1o@simplemachines.org> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(13, "M", null, "225.147.133.159", Seq("F+", "B+"), "Nigeria", "maestro", 60444197038146264L, false, Timestamp.valueOf("2021-11-05 21:32:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <cyule24@tinyurl.com> To: <cblasiak1r@pcworld.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(14, "F", "kstainburnd@bloomberg.com", "116.159.122.197", Seq("D+"), "Russia", "jcb", 3554263324185642L, false, Timestamp.valueOf("2021-11-05 21:33:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <tshepard2p@linkedin.com> To: <epaschkes@unblog.fr> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(15, "F", "dcanacotte@stumbleupon.com", "223.157.153.196", Seq("C+", "G+", "G+"), "China", "switch", 633110630167094910L, false, Timestamp.valueOf("2021-11-05 21:34:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <kpaffordm@toplist.cz> To: <gpercyj@marriott.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(16, "F", null, null, Seq("A+", "B+"), "China", "jcb", 3563463829350803L, true, Timestamp.valueOf("2021-11-05 21:35:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <rmacnockateri@mlb.com> To: <tshepard2p@linkedin.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(17, "M", "sbrunettig@msu.edu", "0.209.16.177", Seq("D+", "B+", "F+", "D+"), "Ukraine", "diners-club-enroute", 201494303155594L, false, Timestamp.valueOf("2021-11-05 21:36:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <mguitton29@opera.com> To: <gthorbon2d@hao123.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(18, "M", "llesurfh@google.pl", "94.154.171.48", Seq("D+"), "Australia", "diners-club-enroute", 201720965596307L, true, Timestamp.valueOf("2021-11-05 21:37:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <palcornp@rambler.ru> To: <eelbyo@gizmodo.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(19, "F", "bkillfordi@cisco.com", "147.204.95.224", Seq("D+", "G+"), "China", "jcb", 3579944029135021L, false, Timestamp.valueOf("2021-11-05 21:38:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <emitrovict@storify.com> To: <freeson2m@indiatimes.com> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])"""),
    FakeData(20, "M", "ehartoppj@istockphoto.com", null, Seq("G+"), "Kuwait", "diners-club-carte-blanche", 30388454305015L, false, Timestamp.valueOf("2021-11-05 21:39:32"), """Mon Mar 19 20:16:27 2018 Info: Bounced: DCID 8413617 MID 19338947 From: <bdilland1@sohu.com> To: <inannetti25@flavors.me> RID 0 - 5.4.7 - Delivery expired (message too old) ("000", ["timeout"])""")
  )
  // scalastyle:on

  val fakeDataForJoin = Seq(
    FakeDataForJoin(1, "Ping Pong"),
    FakeDataForJoin(2, "Rugby"),
    FakeDataForJoin(3, "Football"),
    FakeDataForJoin(4, "Tennis"),
    FakeDataForJoin(5, "Rugby"),
    FakeDataForJoin(6, "Ping Pong"),
    FakeDataForJoin(7, "Rugby"),
    FakeDataForJoin(8, "Football"),
    FakeDataForJoin(9, "Rugby"),
    FakeDataForJoin(10, "Football")
  )

  override def beforeAll(): Unit = {
    import spark.implicits._
    spark.conf.set("spark.sql.parser.quotedRegexColumnNames", value = true)
    spark.createDataset(fakeData).createOrReplaceTempView("fake")
    spark.createDataset(fakeDataForJoin).createOrReplaceTempView("fake_for_join")
  }

  test("bin span execute") {
    executes("index=fake | bin span=5m timestamp | stats count by timestamp | sort timestamp",
      """+-------------------+-----+
        ||timestamp          |count|
        |+-------------------+-----+
        ||2021-11-05 21:20:00|5    |
        ||2021-11-05 21:25:00|5    |
        ||2021-11-05 21:30:00|5    |
        ||2021-11-05 21:35:00|5    |
        |+-------------------+-----+
        |""".stripMargin)
  }

  test("thing2") {
    executes("index=fake | id > 17 | fields + id, gender, email, ipAddress",
      """+---+------+-------------------------+--------------+
        ||id |gender|email                    |ipAddress     |
        |+---+------+-------------------------+--------------+
        ||18 |M     |llesurfh@google.pl       |94.154.171.48 |
        ||19 |F     |bkillfordi@cisco.com     |147.204.95.224|
        ||20 |M     |ehartoppj@istockphoto.com|null          |
        |+---+------+-------------------------+--------------+
        |""".stripMargin)
  }

  test("mvcount(array)") {
    executes("index=fake | eval array_count=mvcount(array) | where array_count=1 " +
      "| fields + id, gender, email, array, array_count",
      """+---+------+-------------------------+-----+-----------+
        ||id |gender|email                    |array|array_count|
        |+---+------+-------------------------+-----+-----------+
        ||2  |M     |null                     |[C+] |1          |
        ||14 |F     |kstainburnd@bloomberg.com|[D+] |1          |
        ||18 |M     |llesurfh@google.pl       |[D+] |1          |
        ||20 |M     |ehartoppj@istockphoto.com|[G+] |1          |
        |+---+------+-------------------------+-----+-----------+
        |""".stripMargin)
  }

  test("mvfilter(len(array) > 2))") {
    executes("index=fake | eval test = mvfilter(len(array) = 2) | fields + array, test",
      """+----------------+----------------+
        ||array           |test            |
        |+----------------+----------------+
        ||[G+, F+]        |[G+, F+]        |
        ||[C+]            |[C+]            |
        ||[A+, A, B, B+]  |[A+, B+]        |
        ||[A+, A+, C+, G+]|[A+, A+, C+, G+]|
        ||[A+, C+]        |[A+, C+]        |
        ||[D+, D+]        |[D+, D+]        |
        ||[D+, C, G+]     |[D+, G+]        |
        ||[C+, C+]        |[C+, C+]        |
        ||[G+, D+]        |[G+, D+]        |
        ||[G, C+, D, D+]  |[C+, D+]        |
        ||[D+, C+, D+, A+]|[D+, C+, D+, A+]|
        ||[D+, D, A, A+]  |[D+, A+]        |
        ||[F+, B+]        |[F+, B+]        |
        ||[D+]            |[D+]            |
        ||[C+, G+, G+]    |[C+, G+, G+]    |
        ||[A+, B+]        |[A+, B+]        |
        ||[D+, B+, F+, D+]|[D+, B+, F+, D+]|
        ||[D+]            |[D+]            |
        ||[D+, G+]        |[D+, G+]        |
        ||[G+]            |[G+]            |
        |+----------------+----------------+
        |""".stripMargin)
  }

  test("mvcount(mvfilter(len(array) > 2)))") {
    executes("index=fake | eval test = mvcount(mvfilter(len(array) = 2)) " +
      "| eval original_count = mvcount(array) " +
      "| where tonumber(original_count) > tonumber(test) | fields + array, original_count, test",
      """+--------------+--------------+----+
        ||array         |original_count|test|
        |+--------------+--------------+----+
        ||[A+, A, B, B+]|4             |2   |
        ||[D+, C, G+]   |3             |2   |
        ||[G, C+, D, D+]|4             |2   |
        ||[D+, D, A, A+]|4             |2   |
        |+--------------+--------------+----+
        |""".stripMargin)
  }


  test("id > len(email) - 10") {
    executes("index=fake | eval email_len = len(email) | id > len(email) - 10 " +
      "| fields + id, email, email_len",
      """+---+-------------------------+---------+
        ||id |email                    |email_len|
        |+---+-------------------------+---------+
        ||9  |wgasnoll8@mit.edu        |17       |
        ||12 |gstureb@nsw.gov.au       |18       |
        ||17 |sbrunettig@msu.edu       |18       |
        ||18 |llesurfh@google.pl       |18       |
        ||19 |bkillfordi@cisco.com     |20       |
        ||20 |ehartoppj@istockphoto.com|25       |
        |+---+-------------------------+---------+
        |""".stripMargin)
  }

  test("eval test = substr(country, 3)") {
    executes("index=fake | eval test = substr(country, 3) | fields country, test",
      """+-----------+---------+
        ||country    |test     |
        |+-----------+---------+
        ||China      |ina      |
        ||China      |ina      |
        ||China      |ina      |
        ||Serbia     |rbia     |
        ||Philippines|ilippines|
        ||Cameroon   |meroon   |
        ||Russia     |ssia     |
        ||Vietnam    |etnam    |
        ||Estonia    |tonia    |
        ||China      |ina      |
        ||Tunisia    |nisia    |
        ||Russia     |ssia     |
        ||Nigeria    |geria    |
        ||Russia     |ssia     |
        ||China      |ina      |
        ||China      |ina      |
        ||Ukraine    |raine    |
        ||Australia  |stralia  |
        ||China      |ina      |
        ||Kuwait     |wait     |
        |+-----------+---------+
        |""".stripMargin)
  }

  test("eval test = substr(country, 3, 3)") {
    executes("index=fake | eval test = substr(country, 3, 3) | fields country, test",
      """+-----------+----+
        ||country    |test|
        |+-----------+----+
        ||China      |ina |
        ||China      |ina |
        ||China      |ina |
        ||Serbia     |rbi |
        ||Philippines|ili |
        ||Cameroon   |mer |
        ||Russia     |ssi |
        ||Vietnam    |etn |
        ||Estonia    |ton |
        ||China      |ina |
        ||Tunisia    |nis |
        ||Russia     |ssi |
        ||Nigeria    |ger |
        ||Russia     |ssi |
        ||China      |ina |
        ||China      |ina |
        ||Ukraine    |rai |
        ||Australia  |str |
        ||China      |ina |
        ||Kuwait     |wai |
        |+-----------+----+
        |""".stripMargin)
  }

  test("eval test = substr(country, -5)") {
    executes("index=fake | eval test = substr(country, -5) | fields country, test",
      """+-----------+-----+
        ||country    |test |
        |+-----------+-----+
        ||China      |China|
        ||China      |China|
        ||China      |China|
        ||Serbia     |erbia|
        ||Philippines|pines|
        ||Cameroon   |eroon|
        ||Russia     |ussia|
        ||Vietnam    |etnam|
        ||Estonia    |tonia|
        ||China      |China|
        ||Tunisia    |nisia|
        ||Russia     |ussia|
        ||Nigeria    |geria|
        ||Russia     |ussia|
        ||China      |China|
        ||China      |China|
        ||Ukraine    |raine|
        ||Australia  |ralia|
        ||China      |China|
        ||Kuwait     |uwait|
        |+-----------+-----+
        |""".stripMargin)
  }

  test("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\"") {
    // scalastyle:off
    executes("index=fake | rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields +_raw, from, to",
      """+------------------------------+------------------------------+------------------------------+
        ||                          _raw|                          from|                            to|
        |+------------------------------+------------------------------+------------------------------+
        ||Mon Mar 19 20:16:27 2018 In...|            twraggsx@phpbb.com|     vstovin2l@tripadvisor.com|
        ||Mon Mar 19 20:16:27 2018 In...|        kquipp1q@newyorker.com|          kannakin13@ameblo.jp|
        ||Mon Mar 19 20:16:27 2018 In...|             tkment1h@usda.gov|          rdeville2e@google.de|
        ||Mon Mar 19 20:16:27 2018 In...|         emaccard1t@rambler.ru|          cyukhtinb@senate.gov|
        ||Mon Mar 19 20:16:27 2018 In...|dswedeland1o@simplemachines...|     vstovin2l@tripadvisor.com|
        ||Mon Mar 19 20:16:27 2018 In...|           rjeaycocke@lulu.com|         jtipler1f@alibaba.com|
        ||Mon Mar 19 20:16:27 2018 In...|         rbaline1b@sina.com.cn|       tbernlin2f@bandcamp.com|
        ||Mon Mar 19 20:16:27 2018 In...|            mbedle6@elpais.com|         jtipler1f@alibaba.com|
        ||Mon Mar 19 20:16:27 2018 In...|bfullilove16@timesonline.co.uk|          cyukhtinb@senate.gov|
        ||Mon Mar 19 20:16:27 2018 In...|           cebbetts2r@1688.com|        hmcgeaney1i@elpais.com|
        ||Mon Mar 19 20:16:27 2018 In...|       mmacelroy18@shop-pro.jp|        ewatton26@buzzfeed.com|
        ||Mon Mar 19 20:16:27 2018 In...|        aiwanowicz1g@yahoo.com|dswedeland1o@simplemachines...|
        ||Mon Mar 19 20:16:27 2018 In...|           cyule24@tinyurl.com|        cblasiak1r@pcworld.com|
        ||Mon Mar 19 20:16:27 2018 In...|       tshepard2p@linkedin.com|           epaschkes@unblog.fr|
        ||Mon Mar 19 20:16:27 2018 In...|          kpaffordm@toplist.cz|          gpercyj@marriott.com|
        ||Mon Mar 19 20:16:27 2018 In...|         rmacnockateri@mlb.com|       tshepard2p@linkedin.com|
        ||Mon Mar 19 20:16:27 2018 In...|          mguitton29@opera.com|         gthorbon2d@hao123.com|
        ||Mon Mar 19 20:16:27 2018 In...|           palcornp@rambler.ru|            eelbyo@gizmodo.com|
        ||Mon Mar 19 20:16:27 2018 In...|        emitrovict@storify.com|      freeson2m@indiatimes.com|
        ||Mon Mar 19 20:16:27 2018 In...|            bdilland1@sohu.com|        inannetti25@flavors.me|
        |+------------------------------+------------------------------+------------------------------+
        |""".stripMargin, truncate = 30)
    // scalastyle:on
  }

  test("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw") {
    executes("index=fake | rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields +from, to",
      """+------------------------------+------------------------------+
        ||                          from|                            to|
        |+------------------------------+------------------------------+
        ||            twraggsx@phpbb.com|     vstovin2l@tripadvisor.com|
        ||        kquipp1q@newyorker.com|          kannakin13@ameblo.jp|
        ||             tkment1h@usda.gov|          rdeville2e@google.de|
        ||         emaccard1t@rambler.ru|          cyukhtinb@senate.gov|
        ||dswedeland1o@simplemachines...|     vstovin2l@tripadvisor.com|
        ||           rjeaycocke@lulu.com|         jtipler1f@alibaba.com|
        ||         rbaline1b@sina.com.cn|       tbernlin2f@bandcamp.com|
        ||            mbedle6@elpais.com|         jtipler1f@alibaba.com|
        ||bfullilove16@timesonline.co.uk|          cyukhtinb@senate.gov|
        ||           cebbetts2r@1688.com|        hmcgeaney1i@elpais.com|
        ||       mmacelroy18@shop-pro.jp|        ewatton26@buzzfeed.com|
        ||        aiwanowicz1g@yahoo.com|dswedeland1o@simplemachines...|
        ||           cyule24@tinyurl.com|        cblasiak1r@pcworld.com|
        ||       tshepard2p@linkedin.com|           epaschkes@unblog.fr|
        ||          kpaffordm@toplist.cz|          gpercyj@marriott.com|
        ||         rmacnockateri@mlb.com|       tshepard2p@linkedin.com|
        ||          mguitton29@opera.com|         gthorbon2d@hao123.com|
        ||           palcornp@rambler.ru|            eelbyo@gizmodo.com|
        ||        emitrovict@storify.com|      freeson2m@indiatimes.com|
        ||            bdilland1@sohu.com|        inannetti25@flavors.me|
        |+------------------------------+------------------------------+
        |""".stripMargin, truncate = 30)
  }

  test("rename country as pays") {
    executes("index=fake | fields +id, gender, country, email | rename country as pays",
      """+---+------+-----------+--------------------------+
        || id|gender|       pays|                     email|
        |+---+------+-----------+--------------------------+
        ||  1|     M|      China|      jcraisford0@imdb.com|
        ||  2|     M|      China|                      null|
        ||  3|     M|      China|       slockyer2@fotki.com|
        ||  4|     F|     Serbia|                      null|
        ||  5|     F|Philippines| abasilotta4@mediafire.com|
        ||  6|     F|   Cameroon|          bhaskins5@w3.org|
        ||  7|     M|     Russia|     arootham6@harvard.edu|
        ||  8|     F|    Vietnam| cmacentee7@mayoclinic.com|
        ||  9|     F|    Estonia|         wgasnoll8@mit.edu|
        || 10|     M|      China|  nhartnup9@opensource.org|
        || 11|     M|    Tunisia|    sflewetta@linkedin.com|
        || 12|     M|     Russia|        gstureb@nsw.gov.au|
        || 13|     M|    Nigeria|                      null|
        || 14|     F|     Russia| kstainburnd@bloomberg.com|
        || 15|     F|      China|dcanacotte@stumbleupon.com|
        || 16|     F|      China|                      null|
        || 17|     M|    Ukraine|        sbrunettig@msu.edu|
        || 18|     M|  Australia|        llesurfh@google.pl|
        || 19|     F|      China|      bkillfordi@cisco.com|
        || 20|     M|     Kuwait| ehartoppj@istockphoto.com|
        |+---+------+-----------+--------------------------+
        |""".stripMargin, truncate = 30)
  }

  test("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields +from, to " +
    "| rename from AS emailFrom, to AS emailTo") {
    executes("index=fake | rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" " +
      "| fields +from, to | rename from AS emailFrom, to AS emailTo",
      """+------------------------------+------------------------------+
        ||                     emailFrom|                       emailTo|
        |+------------------------------+------------------------------+
        ||            twraggsx@phpbb.com|     vstovin2l@tripadvisor.com|
        ||        kquipp1q@newyorker.com|          kannakin13@ameblo.jp|
        ||             tkment1h@usda.gov|          rdeville2e@google.de|
        ||         emaccard1t@rambler.ru|          cyukhtinb@senate.gov|
        ||dswedeland1o@simplemachines...|     vstovin2l@tripadvisor.com|
        ||           rjeaycocke@lulu.com|         jtipler1f@alibaba.com|
        ||         rbaline1b@sina.com.cn|       tbernlin2f@bandcamp.com|
        ||            mbedle6@elpais.com|         jtipler1f@alibaba.com|
        ||bfullilove16@timesonline.co.uk|          cyukhtinb@senate.gov|
        ||           cebbetts2r@1688.com|        hmcgeaney1i@elpais.com|
        ||       mmacelroy18@shop-pro.jp|        ewatton26@buzzfeed.com|
        ||        aiwanowicz1g@yahoo.com|dswedeland1o@simplemachines...|
        ||           cyule24@tinyurl.com|        cblasiak1r@pcworld.com|
        ||       tshepard2p@linkedin.com|           epaschkes@unblog.fr|
        ||          kpaffordm@toplist.cz|          gpercyj@marriott.com|
        ||         rmacnockateri@mlb.com|       tshepard2p@linkedin.com|
        ||          mguitton29@opera.com|         gthorbon2d@hao123.com|
        ||           palcornp@rambler.ru|            eelbyo@gizmodo.com|
        ||        emitrovict@storify.com|      freeson2m@indiatimes.com|
        ||            bdilland1@sohu.com|        inannetti25@flavors.me|
        |+------------------------------+------------------------------+
        |""".stripMargin, truncate = 30)
  }

  test("rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw " +
    "| return 4 emailFrom=from emailTo=to") {
    executes("index=fake | rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" " +
      "| fields - _raw | return 4 emailFrom=from emailTo=to",
      """+----------------------+-------------------------+
        ||             emailFrom|                  emailTo|
        |+----------------------+-------------------------+
        ||    twraggsx@phpbb.com|vstovin2l@tripadvisor.com|
        ||kquipp1q@newyorker.com|     kannakin13@ameblo.jp|
        ||     tkment1h@usda.gov|     rdeville2e@google.de|
        || emaccard1t@rambler.ru|     cyukhtinb@senate.gov|
        |+----------------------+-------------------------+
        |""".stripMargin, truncate = 30)
  }

  test("join type=inner id [search index=fake_for_join]") {
    executes("index=fake | join type=inner id [search index=fake_for_join] " +
      "| fields +id, email, sport",
    """|+---+-------------------------+---------+
        ||id |email                    |sport    |
        |+---+-------------------------+---------+
        ||1  |jcraisford0@imdb.com     |Ping Pong|
        ||2  |null                     |Rugby    |
        ||3  |slockyer2@fotki.com      |Football |
        ||4  |null                     |Tennis   |
        ||5  |abasilotta4@mediafire.com|Rugby    |
        ||6  |bhaskins5@w3.org         |Ping Pong|
        ||7  |arootham6@harvard.edu    |Rugby    |
        ||8  |cmacentee7@mayoclinic.com|Football |
        ||9  |wgasnoll8@mit.edu        |Rugby    |
        ||10 |nhartnup9@opensource.org |Football |
        |+---+-------------------------+---------+
        |""".stripMargin)
  }

  test("join type=inner id [search index=fake_for_join] | fields +id, email, sport") {
    executes("index=fake | join type=left id [search index=fake_for_join] " +
      "| fields +id, email, sport",
      """+---+--------------------------+---------+
        ||id |email                     |sport    |
        |+---+--------------------------+---------+
        ||1  |jcraisford0@imdb.com      |Ping Pong|
        ||2  |null                      |Rugby    |
        ||3  |slockyer2@fotki.com       |Football |
        ||4  |null                      |Tennis   |
        ||5  |abasilotta4@mediafire.com |Rugby    |
        ||6  |bhaskins5@w3.org          |Ping Pong|
        ||7  |arootham6@harvard.edu     |Rugby    |
        ||8  |cmacentee7@mayoclinic.com |Football |
        ||9  |wgasnoll8@mit.edu         |Rugby    |
        ||10 |nhartnup9@opensource.org  |Football |
        ||11 |sflewetta@linkedin.com    |null     |
        ||12 |gstureb@nsw.gov.au        |null     |
        ||13 |null                      |null     |
        ||14 |kstainburnd@bloomberg.com |null     |
        ||15 |dcanacotte@stumbleupon.com|null     |
        ||16 |null                      |null     |
        ||17 |sbrunettig@msu.edu        |null     |
        ||18 |llesurfh@google.pl        |null     |
        ||19 |bkillfordi@cisco.com      |null     |
        ||20 |ehartoppj@istockphoto.com |null     |
        |+---+--------------------------+---------+
        |""".stripMargin)
  }

  test("multisearch") {
    executes("multisearch [index=fake | id < 2 | eval a=5]" +
      " [index=fake | id < 2 | eval a=42]" +
      " [index=fake | id < 2 | eval a=74] | fields +id, a",
      """+---+---+
        ||id |a  |
        |+---+---+
        ||1  |5  |
        ||1  |42 |
        ||1  |74 |
        |+---+---+
        |""".stripMargin)
  }

  test("b_not_null") {
    executes("index=fake | fields +email | eval b_not_null=if(isnotnull(email), 1, 0)",
      """+--------------------------+----------+
        ||email                     |b_not_null|
        |+--------------------------+----------+
        ||jcraisford0@imdb.com      |1         |
        ||null                      |0         |
        ||slockyer2@fotki.com       |1         |
        ||null                      |0         |
        ||abasilotta4@mediafire.com |1         |
        ||bhaskins5@w3.org          |1         |
        ||arootham6@harvard.edu     |1         |
        ||cmacentee7@mayoclinic.com |1         |
        ||wgasnoll8@mit.edu         |1         |
        ||nhartnup9@opensource.org  |1         |
        ||sflewetta@linkedin.com    |1         |
        ||gstureb@nsw.gov.au        |1         |
        ||null                      |0         |
        ||kstainburnd@bloomberg.com |1         |
        ||dcanacotte@stumbleupon.com|1         |
        ||null                      |0         |
        ||sbrunettig@msu.edu        |1         |
        ||llesurfh@google.pl        |1         |
        ||bkillfordi@cisco.com      |1         |
        ||ehartoppj@istockphoto.com |1         |
        |+--------------------------+----------+
        |""".stripMargin)
  }

  test( "null_if_id_gt_3") {
    executes("index=fake | eval id_null=if(id > 10, null(), id) | table id id_null",
      """+---+-------+
        ||id |id_null|
        |+---+-------+
        ||1  |1      |
        ||2  |2      |
        ||3  |3      |
        ||4  |4      |
        ||5  |5      |
        ||6  |6      |
        ||7  |7      |
        ||8  |8      |
        ||9  |9      |
        ||10 |10     |
        ||11 |null   |
        ||12 |null   |
        ||13 |null   |
        ||14 |null   |
        ||15 |null   |
        ||16 |null   |
        ||17 |null   |
        ||18 |null   |
        ||19 |null   |
        ||20 |null   |
        |+---+-------+
        |""".stripMargin)
  }

  test("fillnull") {
    executes("index=fake | fields +id, email | fillnull",
      """+---+--------------------------+
        ||id |email                     |
        |+---+--------------------------+
        ||1  |jcraisford0@imdb.com      |
        ||2  |0                         |
        ||3  |slockyer2@fotki.com       |
        ||4  |0                         |
        ||5  |abasilotta4@mediafire.com |
        ||6  |bhaskins5@w3.org          |
        ||7  |arootham6@harvard.edu     |
        ||8  |cmacentee7@mayoclinic.com |
        ||9  |wgasnoll8@mit.edu         |
        ||10 |nhartnup9@opensource.org  |
        ||11 |sflewetta@linkedin.com    |
        ||12 |gstureb@nsw.gov.au        |
        ||13 |0                         |
        ||14 |kstainburnd@bloomberg.com |
        ||15 |dcanacotte@stumbleupon.com|
        ||16 |0                         |
        ||17 |sbrunettig@msu.edu        |
        ||18 |llesurfh@google.pl        |
        ||19 |bkillfordi@cisco.com      |
        ||20 |ehartoppj@istockphoto.com |
        |+---+--------------------------+
        |""".stripMargin)
  }

  test("fillnull value=NA") {
    executes("index=fake | fields +id, email | fillnull value=NA",
      """+---+--------------------------+
        ||id |email                     |
        |+---+--------------------------+
        ||1  |jcraisford0@imdb.com      |
        ||2  |NA                        |
        ||3  |slockyer2@fotki.com       |
        ||4  |NA                        |
        ||5  |abasilotta4@mediafire.com |
        ||6  |bhaskins5@w3.org          |
        ||7  |arootham6@harvard.edu     |
        ||8  |cmacentee7@mayoclinic.com |
        ||9  |wgasnoll8@mit.edu         |
        ||10 |nhartnup9@opensource.org  |
        ||11 |sflewetta@linkedin.com    |
        ||12 |gstureb@nsw.gov.au        |
        ||13 |NA                        |
        ||14 |kstainburnd@bloomberg.com |
        ||15 |dcanacotte@stumbleupon.com|
        ||16 |NA                        |
        ||17 |sbrunettig@msu.edu        |
        ||18 |llesurfh@google.pl        |
        ||19 |bkillfordi@cisco.com      |
        ||20 |ehartoppj@istockphoto.com |
        |+---+--------------------------+
        |""".stripMargin)
  }

  test("fillnull value=NA a c n valid") {
    executes("index=fake | fields +id, email, gender, ipAddress " +
      "| fillnull value=NA email gender",
      """+---+--------------------------+------+---------------+
        ||id |email                     |gender|ipAddress      |
        |+---+--------------------------+------+---------------+
        ||1  |jcraisford0@imdb.com      |M     |109.177.141.88 |
        ||2  |NA                        |M     |null           |
        ||3  |slockyer2@fotki.com       |M     |165.53.105.69  |
        ||4  |NA                        |F     |156.148.239.162|
        ||5  |abasilotta4@mediafire.com |F     |65.218.253.251 |
        ||6  |bhaskins5@w3.org          |F     |82.24.35.79    |
        ||7  |arootham6@harvard.edu     |M     |95.58.22.253   |
        ||8  |cmacentee7@mayoclinic.com |F     |null           |
        ||9  |wgasnoll8@mit.edu         |F     |null           |
        ||10 |nhartnup9@opensource.org  |M     |4.196.92.130   |
        ||11 |sflewetta@linkedin.com    |M     |129.89.191.159 |
        ||12 |gstureb@nsw.gov.au        |M     |108.79.235.21  |
        ||13 |NA                        |M     |225.147.133.159|
        ||14 |kstainburnd@bloomberg.com |F     |116.159.122.197|
        ||15 |dcanacotte@stumbleupon.com|F     |223.157.153.196|
        ||16 |NA                        |F     |null           |
        ||17 |sbrunettig@msu.edu        |M     |0.209.16.177   |
        ||18 |llesurfh@google.pl        |M     |94.154.171.48  |
        ||19 |bkillfordi@cisco.com      |F     |147.204.95.224 |
        ||20 |ehartoppj@istockphoto.com |M     |null           |
        |+---+--------------------------+------+---------------+
        |""".stripMargin)
  }

  test("eventstats max(n) AS max_n, min(n) by gender") {
    executes("index=fake | eval n = len(email) | fields +id, email, gender, n " +
      "| eventstats max(n) AS max_n, min(n) by gender",
      """+---+--------------------------+------+----+-----+------+
        ||id |email                     |gender|n   |max_n|min(n)|
        |+---+--------------------------+------+----+-----+------+
        ||4  |null                      |F     |null|26   |16    |
        ||5  |abasilotta4@mediafire.com |F     |25  |26   |16    |
        ||6  |bhaskins5@w3.org          |F     |16  |26   |16    |
        ||8  |cmacentee7@mayoclinic.com |F     |25  |26   |16    |
        ||9  |wgasnoll8@mit.edu         |F     |17  |26   |16    |
        ||14 |kstainburnd@bloomberg.com |F     |25  |26   |16    |
        ||15 |dcanacotte@stumbleupon.com|F     |26  |26   |16    |
        ||16 |null                      |F     |null|26   |16    |
        ||19 |bkillfordi@cisco.com      |F     |20  |26   |16    |
        ||1  |jcraisford0@imdb.com      |M     |20  |25   |18    |
        ||2  |null                      |M     |null|25   |18    |
        ||3  |slockyer2@fotki.com       |M     |19  |25   |18    |
        ||7  |arootham6@harvard.edu     |M     |21  |25   |18    |
        ||10 |nhartnup9@opensource.org  |M     |24  |25   |18    |
        ||11 |sflewetta@linkedin.com    |M     |22  |25   |18    |
        ||12 |gstureb@nsw.gov.au        |M     |18  |25   |18    |
        ||13 |null                      |M     |null|25   |18    |
        ||17 |sbrunettig@msu.edu        |M     |18  |25   |18    |
        ||18 |llesurfh@google.pl        |M     |18  |25   |18    |
        ||20 |ehartoppj@istockphoto.com |M     |25  |25   |18    |
        |+---+--------------------------+------+----+-----+------+
        |""".stripMargin)
  }

  test("streamstats count") {
    executes("index=fake | eval _time=timestamp | streamstats count(_time) AS n | " +
      "eval cat=if(n < 4, \"A\",\"B\") | table _time cat n " +
        "| streamstats max(n) AS max_n, min(n) by cat " +
        "| streamstats current=false window=2 min(n) AS min_n_lag",
      """+-------------------+---+---+-----+------+---------+
        ||_time              |cat|n  |max_n|min(n)|min_n_lag|
        |+-------------------+---+---+-----+------+---------+
        ||2021-11-05 21:20:32|A  |1  |1    |1     |null     |
        ||2021-11-05 21:21:32|A  |2  |2    |1     |1        |
        ||2021-11-05 21:22:32|A  |3  |3    |1     |1        |
        ||2021-11-05 21:23:32|B  |4  |4    |4     |2        |
        ||2021-11-05 21:24:32|B  |5  |5    |4     |3        |
        ||2021-11-05 21:25:32|B  |6  |6    |4     |4        |
        ||2021-11-05 21:26:32|B  |7  |7    |4     |5        |
        ||2021-11-05 21:27:32|B  |8  |8    |4     |6        |
        ||2021-11-05 21:28:32|B  |9  |9    |4     |7        |
        ||2021-11-05 21:29:32|B  |10 |10   |4     |8        |
        ||2021-11-05 21:30:32|B  |11 |11   |4     |9        |
        ||2021-11-05 21:31:32|B  |12 |12   |4     |10       |
        ||2021-11-05 21:32:32|B  |13 |13   |4     |11       |
        ||2021-11-05 21:33:32|B  |14 |14   |4     |12       |
        ||2021-11-05 21:34:32|B  |15 |15   |4     |13       |
        ||2021-11-05 21:35:32|B  |16 |16   |4     |14       |
        ||2021-11-05 21:36:32|B  |17 |17   |4     |15       |
        ||2021-11-05 21:37:32|B  |18 |18   |4     |16       |
        ||2021-11-05 21:38:32|B  |19 |19   |4     |17       |
        ||2021-11-05 21:39:32|B  |20 |20   |4     |18       |
        |+-------------------+---+---+-----+------+---------+
        |""".stripMargin)
  }

  test("id > 10 | eval min=min(id,10), max=max(id,15)") {
    executes("index=fake | id > 10 | eval min=min(id,15), max=max(id,15) " +
      "| fields + id, min, max",
      """+---+---+---+
        ||id |min|max|
        |+---+---+---+
        ||11 |11 |15 |
        ||12 |12 |15 |
        ||13 |13 |15 |
        ||14 |14 |15 |
        ||15 |15 |15 |
        ||16 |15 |16 |
        ||17 |15 |17 |
        ||18 |15 |18 |
        ||19 |15 |19 |
        ||20 |15 |20 |
        |+---+---+---+
        |""".stripMargin)
  }

  test("dedup 1 gender static") {
    executes("index=fake | eval static=10 | fields + gender, static " +
      "| dedup 1 gender static",
      """+------+------+
        ||gender|static|
        |+------+------+
        ||M     |10    |
        ||F     |10    |
        |+------+------+
        |""".stripMargin)
  }

  test("inputlookup fake where id < 3") {
    executes("inputlookup fake where id < 3 " +
      "| fields +id, gender, email, ipAddress, country",
      """+---+------+--------------------+--------------+-------+
        ||id |gender|email               |ipAddress     |country|
        |+---+------+--------------------+--------------+-------+
        ||1  |M     |jcraisford0@imdb.com|109.177.141.88|China  |
        ||2  |M     |null                |null          |China  |
        |+---+------+--------------------+--------------+-------+
        |""".stripMargin)
  }

  test("inputlookup max=2 fake where id > 10") {
    executes("inputlookup max=2 fake where id > 10 " +
      "| fields +id, gender, email, ipAddress, country",
      """+---+------+----------------------+--------------+-------+
        ||id |gender|email                 |ipAddress     |country|
        |+---+------+----------------------+--------------+-------+
        ||11 |M     |sflewetta@linkedin.com|129.89.191.159|Tunisia|
        ||12 |M     |gstureb@nsw.gov.au    |108.79.235.21 |Russia |
        |+---+------+----------------------+--------------+-------+
        |""".stripMargin)
  }

  test("format maxresults=2") {
    // scalastyle:off
    executes("index=fake | fields +id, gender, email | format maxresults=2",
      """+----------------------------------------------------------------------------------------------------+
        ||search                                                                                              |
        |+----------------------------------------------------------------------------------------------------+
        ||((id=1) AND (gender=M) AND (email=jcraisford0@imdb.com)) OR ((id=2) AND (gender=M) AND (email=null))|
        |+----------------------------------------------------------------------------------------------------+
        |""".stripMargin)
    // scalastyle:on
  }

  test("format maxresults=2 \"[\" \"[\" \"&&\" \"]\" \"||\" \"]\"") {
    // scalastyle:off
    executes("index=fake | fields +id, gender, email | format maxresults=2 \"[\" \"[\" \"&&\" \"]\" \"||\" \"]\"",
      """+------------------------------------------------------------------------------------------------+
        ||search                                                                                          |
        |+------------------------------------------------------------------------------------------------+
        ||[[id=1] && [gender=M] && [email=jcraisford0@imdb.com]] || [[id=2] && [gender=M] && [email=null]]|
        |+------------------------------------------------------------------------------------------------+
        |""".stripMargin)
    // scalastyle:on
  }

  test("mvcombine country") {
    executes("index=fake | fields +id, country | mvcombine id",
      """+-----------+-------------------------+
        ||country    |id                       |
        |+-----------+-------------------------+
        ||Russia     |[7, 12, 14]              |
        ||Philippines|[5]                      |
        ||China      |[1, 2, 3, 10, 15, 16, 19]|
        ||Kuwait     |[20]                     |
        ||Nigeria    |[13]                     |
        ||Ukraine    |[17]                     |
        ||Estonia    |[9]                      |
        ||Tunisia    |[11]                     |
        ||Cameroon   |[6]                      |
        ||Australia  |[18]                     |
        ||Serbia     |[4]                      |
        ||Vietnam    |[8]                      |
        |+-----------+-------------------------+
        |""".stripMargin)
  }

  test("mvcombine delim=\";\" country") {
    executes("index=fake | fields +id, country | mvcombine delim=\";\" id",
      """+-----------+-----------------+
        ||country    |id               |
        |+-----------+-----------------+
        ||Russia     |7;12;14          |
        ||Philippines|5                |
        ||China      |1;2;3;10;15;16;19|
        ||Kuwait     |20               |
        ||Nigeria    |13               |
        ||Ukraine    |17               |
        ||Estonia    |9                |
        ||Tunisia    |11               |
        ||Cameroon   |6                |
        ||Australia  |18               |
        ||Serbia     |4                |
        ||Vietnam    |8                |
        |+-----------+-----------------+
        |""".stripMargin)
  }

  test("mvexpand d") {
    executes("index=fake | fields +id, array | mvexpand array | where id=1",
      """+---+-----+
        ||id |array|
        |+---+-----+
        ||1  |G+   |
        ||1  |F+   |
        |+---+-----+
        |""".stripMargin)
  }

  test("mvexpand d limit=1") {
    executes("index=fake | fields +id, array | mvexpand array limit=1 | where id=1",
      """+---+-----+
        ||id |array|
        |+---+-----+
        ||1  |G+   |
        |+---+-----+
        |""".stripMargin)
  }

  test("makeresults") {
    executes("makeresults count=5 annotate=t splunk_server_group=\"group1\" " +
      "| fields - _time",
      """+----+----+------+----------+-------------+-------------------+
        ||_raw|host|source|sourcetype|splunk_server|splunk_server_group|
        |+----+----+------+----------+-------------+-------------------+
        ||null|null|null  |null      |local        |group1             |
        ||null|null|null  |null      |local        |group1             |
        ||null|null|null  |null      |local        |group1             |
        ||null|null|null  |null      |local        |group1             |
        ||null|null|null  |null      |local        |group1             |
        |+----+----+------+----------+-------------+-------------------+
        |""".stripMargin)
  }

  test("cidrmatch w/ fct call") {
    executes("index=fake | id < 5 | " +
      "eval in_range = if(cidrmatch(\"109.177.0.0/16\", ipAddress),1,0) | " +
      "fields +id, ipAddress, in_range",
    """+---+---------------+--------+
      ||id |ipAddress      |in_range|
      |+---+---------------+--------+
      ||1  |109.177.141.88 |1       |
      ||2  |null           |0       |
      ||3  |165.53.105.69  |0       |
      ||4  |156.148.239.162|0       |
      |+---+---------------+--------+
      |""".stripMargin)
  }

  test("cidrmatch w/o fct call") {
    executes("index=fake | id < 5 | ipAddress=109.177.0.0/16 | fields +id, ipAddress",
    """+---+--------------+
      ||id |ipAddress     |
      |+---+--------------+
      ||1  |109.177.141.88|
      |+---+--------------+
      |""".stripMargin)
  }

  test("memk fct") {
    executes("index=fake | id < 5 | eval quant=round((id/3),2), unit=if(id < 3, \"M\", \"G\") " +
      "| eval size=quant.unit, memk=memk(size) | fields +id, size, memk",
    """+---+-----+----------+
      ||id |size |memk      |
      |+---+-----+----------+
      ||1  |0.33M|337.92    |
      ||2  |0.67M|686.08    |
      ||3  |1.0G |1048576.0 |
      ||4  |1.33G|1394606.08|
      |+---+-----+----------+
        |""".stripMargin)
  }

  test("rmunit fct") {
    executes("index=fake | id < 5 | eval unit=if(id < 3, \"Megabyte\", \"GB\") " +
      "| eval size=id.unit, rmunit=rmunit(size) | fields +id, size, rmunit",
      """+---+---------+------+
        ||id |size     |rmunit|
        |+---+---------+------+
        ||1  |1Megabyte|1.0   |
        ||2  |2Megabyte|2.0   |
        ||3  |3GB      |3.0   |
        ||4  |4GB      |4.0   |
        |+---+---------+------+
        |""".stripMargin)
  }

  test("rmcomma fct") {
    executes("index=fake | id < 5 | eval s=substr(ipAddress,1,3).\",\".substr(ipAddress, 5, 2)" +
      "| eval n=rmcomma(s) | fields +id, s, n",
      """+---+------+-------+
        ||id |s     |n      |
        |+---+------+-------+
        ||1  |109,17|10917.0|
        ||2  |null  |null   |
        ||3  |165,53|16553.0|
        ||4  |156,14|15614.0|
        |+---+------+-------+
        |""".stripMargin)
  }


  test("num fct") {
    executes("index=fake | id < 5 " +
      "| eval quant=round((id/3),2), unit=if(id < 3, \"M\", \"G\"), fsize=quant.unit " +
      "| eval long_unit=if(id < 3, \"Megabyte\", \"GB\"), id_unit=id.long_unit " +
      "| eval comma_sep=substr(ipAddress,1,3).\",\".substr(ipAddress, 5, 2) " +
      "| convert timeformat =\"%H\" num(id) AS id num(timeStamp) AS hour" +
      "| convert num(fsize) AS fsize_num num(id_unit) AS id_unit_num " +
      "| convert num(comma_sep) AS comma_sep_num num(gender) AS g" +
      "| fields +id, g, hour, id_unit, id_unit_num, fsize, fsize_num, comma_sep, comma_sep_num",
      """+---+----+----+---------+-----------+-----+----------+---------+-------------+
        ||id |g   |hour|id_unit  |id_unit_num|fsize|fsize_num |comma_sep|comma_sep_num|
        |+---+----+----+---------+-----------+-----+----------+---------+-------------+
        ||1.0|null|21  |1Megabyte|1.0        |0.33M|337.92    |109,17   |10917.0      |
        ||2.0|null|21  |2Megabyte|2.0        |0.67M|686.08    |null     |null         |
        ||3.0|null|21  |3GB      |3.0        |1.0G |1048576.0 |165,53   |16553.0      |
        ||4.0|null|21  |4GB      |4.0        |1.33G|1394606.08|156,14   |15614.0      |
        |+---+----+----+---------+-----------+-----+----------+---------+-------------+
        |""".stripMargin)
  }

  test("convert w/ timeformat=\"%H\" ctime(timestamp)") {
    executes("index=fake | id < 3 | convert timeformat=\"%H\" ctime(timestamp) AS hour " +
      "| fields +id, timestamp, hour",
      """+---+-------------------+----+
        ||id |timestamp          |hour|
        |+---+-------------------+----+
        ||1  |2021-11-05 21:20:32|21  |
        ||2  |2021-11-05 21:21:32|21  |
        |+---+-------------------+----+
        |""".stripMargin)
  }

  test("convert w/o timeformat ctime(timestamp)") {
    executes("index=fake | id < 3 | convert ctime(timestamp) AS ctime " +
      "| fields +id, timestamp, ctime",
      """+---+-------------------+-------------------+
        ||id |timestamp          |ctime              |
        |+---+-------------------+-------------------+
        ||1  |2021-11-05 21:20:32|11/05/2021 21:20:32|
        ||2  |2021-11-05 21:21:32|11/05/2021 21:21:32|
        |+---+-------------------+-------------------+
        |""".stripMargin)
  }

  test("convert w/ wildcard") {
    executes("index=fake | id < 3 | fields +id, cardType, cardNumber" +
      "| convert num(card*) none(cardType)",
      """+---+--------+--------------------+
        ||id |cardType|cardNumber          |
        |+---+--------+--------------------+
        ||1  |maestro |6.304276470412087E15|
        ||2  |jcb     |3.538391327116529E15|
        |+---+--------+--------------------+
        |""".stripMargin)
  }

  test("convert w/ wildcard auto(*) none(id*)") {
    executes("index=fake | id < 3 | fields +id, ipAddress, cardType, cardNumber" +
      "| convert auto(*) none(i*)",
      """+---+--------------+--------+--------------------+
        ||id |ipAddress     |cardType|cardNumber          |
        |+---+--------------+--------+--------------------+
        ||1  |109.177.141.88|maestro |6.304276470412087E15|
        ||2  |null          |jcb     |3.538391327116529E15|
        |+---+--------------+--------+--------------------+
        |""".stripMargin)
  }

  test("eval date=strftime(timeStamp)") {
    executes("index=fake | id < 3 | eval date = strftime(timeStamp, \"%m/%d/%Y %H:%M:%S\")" +
      "| eval hour=strftime(\"2021-11-05 21:20:32\", \"%H\") | fields +id, date, hour",
      """+---+-------------------+----+
        ||id |date               |hour|
        |+---+-------------------+----+
        ||1  |11/05/2021 21:20:32|21  |
        ||2  |11/05/2021 21:21:32|21  |
        |+---+-------------------+----+
        |""".stripMargin)
  }

  test("addtotals") {
    executes("index=fake | eval anotherNum=10 | fields +id, gender, anotherNum " +
      "| addtotals fieldname=my_total",
      """+---+------+----------+--------+
        ||id |gender|anotherNum|my_total|
        |+---+------+----------+--------+
        ||1  |M     |10        |11.0    |
        ||2  |M     |10        |12.0    |
        ||3  |M     |10        |13.0    |
        ||4  |F     |10        |14.0    |
        ||5  |F     |10        |15.0    |
        ||6  |F     |10        |16.0    |
        ||7  |M     |10        |17.0    |
        ||8  |F     |10        |18.0    |
        ||9  |F     |10        |19.0    |
        ||10 |M     |10        |20.0    |
        ||11 |M     |10        |21.0    |
        ||12 |M     |10        |22.0    |
        ||13 |M     |10        |23.0    |
        ||14 |F     |10        |24.0    |
        ||15 |F     |10        |25.0    |
        ||16 |F     |10        |26.0    |
        ||17 |M     |10        |27.0    |
        ||18 |M     |10        |28.0    |
        ||19 |F     |10        |29.0    |
        ||20 |M     |10        |30.0    |
        |+---+------+----------+--------+
        |""".stripMargin)
  }
}
