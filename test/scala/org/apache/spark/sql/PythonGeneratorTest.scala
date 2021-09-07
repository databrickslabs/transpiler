package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.scalatest.Outcome
import org.scalatest.concurrent.TimeLimits.failAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

class PythonGeneratorTest extends AnyFunSuite {
  private val src: UnresolvedRelation = UnresolvedRelation(Seq("src"))

  test(".write.saveAsTable('dst', mode='append')") {
    g(AppendData.byName(
      UnresolvedRelation(Seq("dst")),
      src
    ))
  }

  test(".selectExpr('a', 'b')") {
    g(Project(
      Seq(
        UnresolvedAttribute("a"),
        UnresolvedAttribute("b")
      ),
      src)
    )
  }

  // TODO: this test is a bit wrong, fix the logic
  test(".selectExpr('1 AS a', '1 AS b')\n.withColumn('a', F.lit(1))") {
    g(Project(
      Seq(
        // TODO: make another case with UnresolvedAttribute here
        Alias(Literal.create(1), "a")(),
      ),
      Project(
        Seq(
          Alias(Literal.create(1), "a")(),
          Alias(Literal.create(1), "b")(),
        ),
        src
      ))
    )
  }

  test(".where('((`a` = 1) OR (`b` = 2))')") {
    g(Filter(
      Or(
        EqualTo(UnresolvedAttribute("a"), Literal.create(1)),
        EqualTo(UnresolvedAttribute("b"), Literal.create(2))
      ), src))
  }

  test(".where('(`a` = 1)')\n.where('(`b` = 2)')") {
    g(Filter(
      And(
        EqualTo(UnresolvedAttribute("a"), Literal.create(1)),
        EqualTo(UnresolvedAttribute("b"), Literal.create(2))
      ), src))
  }

  test(".limit(10)") {
    g(Limit(Literal.create(10), src))
  }

  private def g(plan: LogicalPlan): Unit = {
    val code = new PythonGenerator().fromPlan(plan)
        // replace src shim to make tests readable
        .replace("spark.table('src')\n", "")
    assert(code == currentTest)
  }

  val timeLimit: Span = 300000 millis

  var currentTest: String = _
  override def withFixture(test: NoArgTest): Outcome = {
    failAfter(timeLimit) {
      this.synchronized {
        currentTest = test.name
        super.withFixture(test)
      }
    }
  }
}
