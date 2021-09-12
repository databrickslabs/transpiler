package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.catalyst.plans.{LeftOuter, PlanTestBase, UsingJoin}


class SplToCatalystTest extends AnyFunSuite with PlanTestBase {
    test("HeadCommand should generate a Limit") {
        check(spl.HeadCommand(
            spl.IntValue(10),
            spl.Bool(false),
            spl.Bool(false)),
        (_, tree) =>
            Limit(Literal(10), tree))
    }

    test("HeadCommand should generate a Filter") {
        check(spl.HeadCommand(
            spl.Binary(
                spl.Value("count"),
                spl.GreaterThan,
                spl.IntValue(10))),
        (_, tree) =>
            Filter(
                GreaterThan(
                    UnresolvedAttribute("count"),
                    Literal(10)),
                tree))
    }

    test("SortCommand command should generate a Sort") {
        check(spl.SortCommand(Seq(
                Tuple2(Some("+"), spl.Value("A")),
                Tuple2(Some("-"), spl.Value("B")),
                Tuple2(Some("+"), spl.Call("num", Seq(spl.Value("C")))))),
        (_, tree) =>
            Sort(Seq(
                SortOrder(UnresolvedAttribute("A"), Ascending),
                SortOrder(UnresolvedAttribute("B"), Descending),
                SortOrder(Cast(UnresolvedAttribute("C"), DoubleType), Ascending)
            ), global =  true, tree))
    }

    test("SortCommand command should generate another Sort") {
        check(spl.SortCommand(Seq(
            Tuple2(None, spl.Value("A")))),
        (_, tree) =>
            Sort(Seq(
                SortOrder(UnresolvedAttribute("A"), Ascending)
            ), global =  true, tree))
    }

    test("FieldsCommand should generate a Project with UnresolvedAlias") {
        check(spl.FieldsCommand(
            None, Seq(
                spl.Value("colA"),
                spl.Value("colB"))),
        (_, tree) =>
            Project(Seq(
                Column("colA").named,
                Column("colB").named,
            ), tree))
    }

    test("FieldsCommand should generate another Project with with UnresolvedAlias") {
        check(spl.FieldsCommand(
            Some("+"), Seq(
                spl.Value("colA"),
                spl.Value("colB"),
                spl.Value("colC"))),
        (_, tree) =>
            Project(Seq(
                Column("colA").named,
                Column("colB").named,
                Column("colC").named,
            ), tree))
    }

    test("FieldsCommand should generate a Project with UnresolvedRegex") {
        check(spl.FieldsCommand(
            Some("-"), Seq(
                spl.Value("colA"),
                spl.Value("colB"))),
        (_, tree) =>
            Project(Seq(
                UnresolvedRegex("(?!$colA|colB).*",
                    None, caseSensitive = false)
            ), tree))
    }

    test("StatsCommand to Aggregate") {
        check(spl.StatsCommand(
            Map(),
            Seq(spl.Call("count", Seq())),
            Seq(spl.Value("host"))),
        (_, tree) =>
            Aggregate(
                Seq(Column("host").named),
                Seq(Alias(Count(Seq()), "count")()),
                tree))
    }

    test("StatsCommand to Deduplicate Aggregate") {
        check(spl.StatsCommand(
            Map(),
            Seq(spl.Call("count", Seq())),
            Seq(spl.Value("host")),
            dedupSplitVals = true),
            (_, tree) =>
                Deduplicate(
                    Seq(
                        UnresolvedAttribute("host")
                    ),
                    Aggregate(
                        Seq(Column("host").named),
                        Seq(Alias(Count(Seq()), "count")()),
                        tree)))
    }

    test("LookupCommand to Join") {
        check(spl.LookupCommand(
            "dst", Seq(
                spl.Value("a"),
                spl.AliasedField(spl.Value("b"), "c")
            ), None),
        (_, tree) =>
            Join(tree,
                Project(Seq(
                    UnresolvedAttribute("a"),
                    Alias(UnresolvedAttribute("b"), "c")()
                ), UnresolvedRelation(Seq("dst"))),
                UsingJoin(LeftOuter, Seq("a", "c")),
                None, JoinHint.NONE))
    }

    test("Rex Command should generate a Project") {
        check(spl.RexCommand(
            Some(spl.Value("colNameA")),
            None,
            None,
            None,
            "From: <(?<from>.*)> To: <(?<to>.*)>"),
        (_, tree) =>
            Project(Seq(
                UnresolvedRegex("^.*?", None, caseSensitive = false),
                Alias(RegExpExtract(
                    Column("colNameA").expr,
                    Literal("From: <(?<from>.*)> To: <(?<to>.*)>"),
                    Literal(1)), "from")(),
                Alias(RegExpExtract(
                    Column("colNameA").expr,
                    Literal("From: <(?<from>.*)> To: <(?<to>.*)>"),
                    Literal(2)), "to")(),
        ), tree))
    }

    test("Rex Command should throw an error") {
        val command = spl.RexCommand(
            Some(spl.Value("colNameA")),
            None,
            None,
            Some(spl.Value("sed")),
            "s/(\\d{4}-){3}/XXXX-XXXX-XXXX-/g")

        assertPlanThrows(command, new NotImplementedError)
    }

    test("rename command should generate a Project") {
        check(spl.RenameCommand(Seq(
            spl.Alias(
                spl.Value("colNameA"),
                "colARenamed"))),
        (_, tree) =>
            Project(Seq(
                UnresolvedRegex("(?!$colNameA).*", None, caseSensitive = false),
                Alias(Column("colNameA").expr, "colARenamed")()
            ), tree)
        )
    }

    test("rename command should generate another Project") {
        check(spl.RenameCommand(Seq(
            spl.Alias(
                spl.Value("colNameA"),
                "colARenamed"),
            spl.Alias(
                spl.Value("colNameB"),
                "colBRenamed"))),
            (_, tree) =>
                Project(Seq(
                    UnresolvedRegex("(?!$colNameA|colNameB).*", None, caseSensitive = false),
                    Alias(Column("colNameA").expr, "colARenamed")(),
                    Alias(Column("colNameB").expr, "colBRenamed")()
                ), tree)
        )
    }


    test("regex command should generate a Filter") {
        check(spl.RegexCommand(
            Some((spl.Value("colNameA"), "!=")),
            "[0-9]{5}(-[0-9]{4})?"),
            (_, tree) =>
                Filter(Not(
                    RLike(
                        Column("colNameA").expr,
                        Literal("[0-9]{5}(-[0-9]{4})?"))
                ), tree)
        )
    }

    private def check(command: spl.Command,
              callback: (spl.Command, LogicalPlan) => LogicalPlan
              ): Unit = this.synchronized {
        val pipeline = spl.Pipeline(Seq(command))
        val actualPlan: LogicalPlan = new SplToCatalyst().process(pipeline)
        val expectedPlan = pipeline.commands.foldLeft(
            UnresolvedRelation(Seq("x")).asInstanceOf[LogicalPlan]) {
            (tree, cmd) => callback(cmd, tree)
        }
        comparePlans(actualPlan, expectedPlan, checkAnalysis = false)
    }

    private def assertPlanThrows(command: spl.Command, error: Throwable): Unit = {
        val pipeline = spl.Pipeline(Seq(command))
        assertThrows[NotImplementedError] {
            new SplToCatalyst().process(pipeline)
        }
    }
}

