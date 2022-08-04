package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.Filter

case class Dummy(a: String, b: String, c: String, n: Int, valid: Boolean)

case class FakeData(id: Int,
                    gender: String,
                    email: String,
                    ipAddress: String,
                    array: Seq[String],
                    country: String,
                    cardType: String,
                    cardNumber: Long,
                    isStolen: Boolean,
                    timestamp: java.sql.Timestamp,
                    _raw: String)

case class FakeDataForJoin(id: Int, sport: String)
case class FakeDataWithDoubleValues(id: Int, score: Double)

class SplExtensionTest extends AnyFunSuite with ProcessProxy {
  val dummy = Seq(
    Dummy("a", "b", "10.0.0.64", 1, valid = true),
    Dummy("d", "e", "10.0.0.65", 2, valid = false),
    Dummy("g", "h", "10.0.0.66", 3, valid = true),
    Dummy("h", "g", "10.0.0.67", 4, valid = false),
    Dummy("e", "d", "10.0.0.255", 5, valid = true)
  )

  import spark.implicits._
  val dataFrame: Dataset[Dummy] = spark.createDataset(dummy)

  test("term(...) should apply filtering") {
    val result =
      dataFrame
        .where("TERM('e') OR term('a')")
        .select("n")
    result.show()

    val rows = result.select("n").as[Int].collect()
    rows mustEqual Seq(1, 2, 5)
  }

  test("term(...) should raise an exception if more than one parameter is specified") {
    assertThrows[AnalysisException] {
      dataFrame
        .where("TERM('e', 'n')")
        .select("n")
        .show()
    }
  }

  test("cidrmatch(...) should match a CIDR") {
    val result =
      dataFrame
        .where(F.expr("cidr_match('10.0.0.128/4', c)"))
        .select("n")
    result.show()

    val rows = result.select("n").as[Int].collect()
    rows mustEqual Seq(5)
  }

  test("cidrmatch(...) should raise an exception if more/less than 2 paramters are specified") {
    assertThrows[AnalysisException] {
      dataFrame
        .where(F.expr("cidr_match('10.0.0.128/4', c, n)"))
        .select("n")
        .show()
    }
  }

  test("FillNullShim output(...) method should raise an `UnresolvedException`") {
    val shimToTest = FillNullShim("0", Set("id", "score"), dataFrame.logicalPlan)
    assertThrows[UnresolvedException[FillNullShim]] {
      shimToTest.output
    }
  }

  test("Term class nullable() method should always return `false`") {
    assert(!Term(UnresolvedAttribute("n")).nullable)
  }

  test("TermExpansion should raise an `AnalysisException` if term(...) parameter is not resolved") {
    val termWithUnresolvedChildPlan = Filter(
      Term(UnresolvedAttribute("test")),
      UnresolvedRelation(Seq("main")))

    val caught = intercept[AnalysisException] {
      Dataset.ofRows(spark, termWithUnresolvedChildPlan)
    }

    assert(caught.getMessage().contains("Child expression must be resolved"))
  }
}
