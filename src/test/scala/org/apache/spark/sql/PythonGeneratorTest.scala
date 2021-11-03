package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRegex, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, Complete, Count, Max}
import org.apache.spark.sql.catalyst.plans.{LeftOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.types.DoubleType
import org.scalactic.source.Position
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

  test(".select(F.col('a'), F.col('b'))") {
    g(Project(
      Seq(
        UnresolvedAttribute("a"),
        UnresolvedAttribute("b")
      ),
      src)
    )
  }

  // TODO: this test is a bit wrong, fix the logic
  test(".select(F.lit(1).alias('a'), F.lit(1).alias('b'))\n.withColumn('a', F.lit(1))") {
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

  test(".where('a = 1')\n.where('b = 2')") {
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

  test(".orderBy(F.col('a').cast('double').asc())") {
    g(Sort(Seq(
      SortOrder(Cast(Column("a").expr, DoubleType), Ascending)
    ), global = true, src))
  }

  test(".groupBy('host')\n.agg(F.count().alias('count'))") {
    g(Aggregate(
      Seq(Column("host").named),
      Seq(Alias(Count(Seq()),
        "count")()),
      src))
  }

  test(".join(spark.table('dst')\n.withColumn('c', F.col('b')),\n['c'], 'left_outer')") {
    g(Join(src,
      Project(Seq(
        Alias(UnresolvedAttribute("b"), "c")()
      ), UnresolvedRelation(Seq("dst"))),
      UsingJoin(LeftOuter, Seq("c")),
      None, JoinHint.NONE))
  }

  test(".withColumn('from', F.regexp_extract(F.col('event_type'), 'From: <(?<from>.*)> To: <(?<to>.*)>', 1))") {
    g(
      Project(
        Seq(Alias(
          RegExpExtract(
            Column("event_type").expr,
            Literal("From: <(?<from>.*)> To: <(?<to>.*)>"),
            Literal(1)), "from")()), src)
    )
  }

  test(""".select(F.regexp_extract(F.col('event_type'), 'From: <(?<from>.*)> To: <(?<to>.*)>', 1).alias('from'),
         |  F.regexp_extract(F.col('event_type'), 'From: <(?<from>.*)> To: <(?<to>.*)>', 2).alias('to'))""".stripMargin) {
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

  test(".selectExpr('`^(?!event).*$`')") {
    g(
      Project(
        Seq(
          UnresolvedRegex("^(?!event).*$", None, caseSensitive = false)
        ), src)
    )
  }

  test(".select(F.col('`^(?!event_type).*`'), F.col('event_type').alias('testRenamed'))") {
    g(
      Project(
        Seq(
          UnresolvedRegex("^(?!event_type).*", None, caseSensitive = false),
          Alias(Column("event_type").expr, "testRenamed")()
        ), src)
    )
  }

  test(".na.fill('n/a', ['event_type', 'event_id'])") {
    g(FillNullShim("n/a", Set("event_type", "event_id"), src))
  }

  test(".na.fill('n/a')") {
    g(FillNullShim("n/a", Set.empty[String], src))
  }

  test(
    (".withColumn('minimum', F.max(F.col('colA')).over(" +
        "Window.partitionBy(F.col('colB')).orderBy(F.col('colC').asc())))").stripMargin) {
    g(
      Project(Seq(
        Alias(
          WindowExpression(
            AggregateExpression(Max(UnresolvedAttribute("colA")), Complete, isDistinct = false),
            WindowSpecDefinition(
              Seq(UnresolvedAttribute("colB")),
              Seq(SortOrder(UnresolvedAttribute("colC"), Ascending)),
              UnspecifiedFrame
            )
          ), "minimum")()
      ), src)
    )
  }

  test(".limit(10)\n.groupBy()\n.agg(F.array_join(F.collect_list(" +
      "F.format_string('((a=%s) AND (b=%s))', F.col('a'), F.col('b'))), 'OR').alias('search'))") {
    g(
      Aggregate(
        Seq(),
        Seq(
          Alias(
            ArrayJoin(
              AggregateExpression(
                CollectList(
                  FormatString((Literal("((a=%s) AND (b=%s))") +: Seq(
                    UnresolvedAttribute("a"),
                    UnresolvedAttribute("b")
                  ): _*))
                ), Complete, isDistinct = false),
                Literal("OR"),
                None
            ), "search")()
        ),
        Limit(Literal(10), src)
      )
    )
  }

  test(".groupBy('ip', 'port')\n.agg(F.array_join(F.collect_list(F.col('host')), ',').alias('host'))") {
    g(
      Aggregate(
        Seq(
          UnresolvedAttribute("ip"),
          UnresolvedAttribute("port")
        ),
        Seq(
          UnresolvedAttribute("ip"),
          UnresolvedAttribute("port"),
          Alias(
            ArrayJoin(
              AggregateExpression(
                CollectList(UnresolvedAttribute("host")),
                Complete,
                isDistinct = false
              ),
              Literal(","),
              None
            ) , "host")()
        ) , src)
    )
  }

  private def g(plan: LogicalPlan)(implicit pos: Position): Unit = {
    val code = PythonGenerator.fromPlan(GeneratorContext(), plan)
        // replace src shim to make tests readable
        .replace("spark.table('src')\n", "")
    if (code != currentTest) {
      fail(s"""FAILURE: Code does not match
              |=======
              |${sideBySide(code, currentTest).mkString("\n")}
              |""".stripMargin)
    }
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
