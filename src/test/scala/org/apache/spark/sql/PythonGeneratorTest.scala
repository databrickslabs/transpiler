package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRegex, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.{LeftOuter, UsingJoin}
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

  test(".orderBy(F.col('a').desc())") {
    g(Sort(Seq(
      SortOrder(UnresolvedAttribute("a"), Descending)
    ), global = true, src))
  }

//  test(".orderBy(F.col('a').cast('double').asc())") {
//    g(Sort(Seq(
//      SortOrder(Cast("a"), Descending)
//    ), global = true, src))
//  }

  test(".groupBy('host')\n.agg(F.expr('count() AS `count`'))") {
    g(Aggregate(
      Seq(Column("host").named),
      Seq(Alias(Count(Seq()),
        "count")()),
      src))
  }

  test(".join(spark.table('dst')\n.withColumn('c', F.expr('`b`')),\n['c'], 'left_outer')") {
    g(Join(src,
      Project(Seq(
        Alias(UnresolvedAttribute("b"), "c")()
      ), UnresolvedRelation(Seq("dst"))),
      UsingJoin(LeftOuter, Seq("c")),
      None, JoinHint.NONE))
  }

  test(".withColumn('from', F.expr(\"regexp_extract(`event_type`, 'From: <(?<from>.*)> To: <(?<to>.*)>', 1)\"))") {
    g(
      Project(
        Seq(Alias(
          RegExpExtract(
            Column("event_type").expr,
            Literal("From: <(?<from>.*)> To: <(?<to>.*)>"),
            Literal(1)), "from")()), src)
    )
  }

  test(".selectExpr(\"regexp_extract(`event_type`, 'From: <(?<from>.*)> To: <(?<to>.*)>', 1) AS from\", " +
                   "\"regexp_extract(`event_type`, 'From: <(?<from>.*)> To: <(?<to>.*)>', 2) AS to\")") {
    g(
      Project(
        Seq(
          Alias(RegExpExtract(
            Column("event_type").expr,
            Literal("From: <(?<from>.*)> To: <(?<to>.*)>"),
            Literal(1)), "from")(),
          Alias(RegExpExtract(
            Column("event_type").expr,
            Literal("From: <(?<from>.*)> To: <(?<to>.*)>"),
            Literal(2)), "to")()
        ), src),

    )
  }

  test(".selectExpr('`^(?!$event).*$`')") {
    g(
      Project(
        Seq(
          UnresolvedRegex("^(?!$event).*$", None, caseSensitive = false)
        ), src)
    )
  }

  test(".selectExpr('`^(?!event_type).*`', 'event_type AS testRenamed')") {
    g(
      Project(
        Seq(
          UnresolvedRegex("^(?!event_type).*", None, caseSensitive = false),
          Alias(Column("event_type").expr, "testRenamed")()
        ), src)
    )
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
