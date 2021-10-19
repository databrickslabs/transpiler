package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, PlanTestBase, UsingJoin}


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
                spl.Field("count"),
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
                Tuple2(Some("+"), spl.Field("A")),
                Tuple2(Some("-"), spl.Field("B")),
                Tuple2(Some("+"), spl.Call("num", Seq(spl.Field("C")))))),
        (_, tree) =>
            Sort(Seq(
                SortOrder(UnresolvedAttribute("A"), Ascending),
                SortOrder(UnresolvedAttribute("B"), Descending),
                SortOrder(Cast(UnresolvedAttribute("C"), DoubleType), Ascending)
            ), global =  true, tree))
    }

    test("SortCommand command should generate another Sort") {
        check(spl.SortCommand(Seq(
            Tuple2(None, spl.Field("A")))),
        (_, tree) =>
            Sort(Seq(
                SortOrder(UnresolvedAttribute("A"), Ascending)
            ), global =  true, tree))
    }

    test("FieldsCommand should generate a Project with UnresolvedAlias") {
        check(spl.FieldsCommand(
            None, Seq(
                spl.Field("colA"),
                spl.Field("colB"))),
        (_, tree) =>
            Project(Seq(
                UnresolvedAttribute("colA"),
                UnresolvedAttribute("colB"),
            ), tree))
    }

    test("FieldsCommand should generate another Project with with UnresolvedAlias") {
        check(spl.FieldsCommand(
            Some("+"), Seq(
                spl.Field("colA"),
                spl.Field("colB"),
                spl.Field("colC"))),
        (_, tree) =>
            Project(Seq(
                UnresolvedAttribute("colA"),
                UnresolvedAttribute("colB"),
                UnresolvedAttribute("colC"),
            ), tree))
    }

    test("FieldsCommand should generate a Project with UnresolvedRegex") {
        check(spl.FieldsCommand(
            Some("-"), Seq(
                spl.Field("colA"),
                spl.Field("colB"))),
        (_, tree) =>
            Project(Seq(
                UnresolvedRegex("(?!colA|colB).*",
                    None, caseSensitive = false)
            ), tree))
    }

    test("StatsCommand to Aggregate") {
        check(spl.StatsCommand(
            Map(),
            Seq(spl.Call("count", Seq())),
            Seq(spl.Field("host"))),
        (_, tree) =>
            Aggregate(
                Seq(UnresolvedAttribute("host")),
                Seq(Alias(Count(Seq()), "count")()),
                tree))
    }

    test("StatsCommand to Deduplicate Aggregate") {
        check(spl.StatsCommand(
            Map(),
            Seq(spl.Call("count", Seq())),
            Seq(spl.Field("host")),
            dedupSplitVals = true),
            (_, tree) =>
                Deduplicate(
                    Seq(
                        UnresolvedAttribute("host")
                    ),
                    Aggregate(
                        Seq(UnresolvedAttribute("host")),
                        Seq(Alias(Count(Seq()), "count")()),
                        tree)))
    }

    test("LookupCommand to Join") {
        check(spl.LookupCommand(
            "dst", Seq(
                spl.Field("a"),
                spl.AliasedField(spl.Field("b"), "c")
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
            Some("colNameA"),
            1,
            None,
            None,
            "From: <(?<from>.*)> To: <(?<to>.*)>"),
        (_, tree) =>
            Project(Seq(
                UnresolvedAttribute("_raw"),
                Alias(RegExpExtract(
                    UnresolvedAttribute("colNameA"),
                    Literal("From: <(?<from>.*)> To: <(?<to>.*)>"),
                    Literal(1)), "from")(),
                Alias(RegExpExtract(
                    UnresolvedAttribute("colNameA"),
                    Literal("From: <(?<from>.*)> To: <(?<to>.*)>"),
                    Literal(2)), "to")()
            ), Project(Seq(
                UnresolvedAttribute("_raw"),
                Alias(RegExpExtract(
                    UnresolvedAttribute("colNameA"),
                    Literal("From: <(?<from>.*)> To: <(?<to>.*)>"),
                    Literal(1)), "from")(),
            ), Project(Seq(
                UnresolvedAttribute("_raw")
            ), tree))))
    }

    test("Rex Command should throw an error") {
        check(spl.RexCommand(
            Some("colNameA"),
            1,
            None,
            Some("sed"),
            "s/(\\d{4}-){3}/XXXX-XXXX-XXXX-/g"),
        (_, tree) =>
            UnknownPlanShim(
                "Error in rex: scala.NotImplementedError: rex mode=sed currently not supported!",
                tree))
    }

    test("rename command should generate a Project") {
        check(spl.RenameCommand(Seq(
            spl.Alias(
                spl.Field("colNameA"),
                "colARenamed"))),
        (_, tree) =>
            Project(Seq(
                UnresolvedRegex("(?!colNameA).*", None, caseSensitive = false),
                Alias(Column("colNameA").expr, "colARenamed")()
            ), tree)
        )
    }

    test("rename command should generate another Project") {
        check(spl.RenameCommand(Seq(
            spl.Alias(
                spl.Field("colNameA"),
                "colARenamed"),
            spl.Alias(
                spl.Field("colNameB"),
                "colBRenamed"))),
            (_, tree) =>
                Project(Seq(
                    UnresolvedRegex("(?!colNameA|colNameB).*", None, caseSensitive = false),
                    Alias(Column("colNameA").expr, "colARenamed")(),
                    Alias(Column("colNameB").expr, "colBRenamed")()
                ), tree)
        )
    }


    test("regex command should generate a Filter") {
        check(spl.RegexCommand(
            Some((spl.Field("colNameA"), "!=")),
            "[0-9]{5}(-[0-9]{4})?"),
            (_, tree) =>
                Filter(Not(
                    RLike(
                        Column("colNameA").expr,
                        Literal("[0-9]{5}(-[0-9]{4})?"))
                ), tree)
        )
    }

    test("return 10 ip host port") {
        // Adding a comment
        check(spl.ReturnCommand(
            Some(spl.IntValue(10)),
            Seq(spl.Field("ip"),
                spl.Field("host"),
                spl.Field("port"))),
        (_, tree) => Limit(Literal(10), Project(Seq(
            Column("ip").named,
            Column("host").named,
            Column("port").named)
        , tree))
        )
    }

    test("return 10 ip $env $test") {
        check(spl.ReturnCommand(
            Some(spl.IntValue(10)),
            Seq(spl.Field("env"),
                spl.Field("test"))),
            (_, tree) => Limit(Literal(10), Project(Seq(
                Column("env").named,
                Column("test").named)
            , tree))
        )
    }

    test("return 20 a=ip b=host c=port") {
        check(spl.ReturnCommand(
            Some(spl.IntValue(20)),
            Seq((spl.Field("a"), spl.Field("ip")),
                (spl.Field("b"), spl.Field("host")),
                (spl.Field("c"), spl.Field("port")))),
            (_, tree) => Limit(Literal(20), Project(Seq(
                Alias(Column("ip").named, "a")(),
                Alias(Column("host").named, "b")(),
                Alias(Column("port").named, "c")())
            , tree))
        )
    }

    test("join product_id [search vendors]") {
        check(spl.JoinCommand(
            joinType = "inner",
            useTime = false,
            earlier = true,
            overwrite = false,
            max = 1,
            Seq(spl.Field("product_id")),
            spl.Pipeline(Seq(
                spl.SearchCommand(spl.Field("vendors"))
            ))),
            (_, tree) => {
                Join(tree,
                     Filter(Literal("vendors"), tree),
                     UsingJoin(Inner, List("product_id")), None, JoinHint.NONE)
            }
        )
    }

    test("join type=left product_id product_name [search vendors]") {
        check(spl.JoinCommand(
            joinType = "left",
            useTime = false,
            earlier = true,
            overwrite = false,
            max = 1,
            Seq(spl.Field("product_id"), spl.Field("product_name")),
            spl.Pipeline(Seq(
                spl.SearchCommand(spl.Field("vendors"))
            ))),
            (_, tree) => {
                Join(tree,
                    Filter(Literal("vendors"), tree),
                    UsingJoin(LeftOuter, List("product_id", "product_name")), None, JoinHint.NONE)
            }
        )
    }

    test("fillnull") {
        check(spl.FillNullCommand(
            Some("0"),
            None
            ),
            (_, tree) => {
                FillNullShim("0", Set.empty[String], tree)
            }
        )
    }

    test("fillnull value=\"NaN\" host port ip") {
        check(spl.FillNullCommand(
            Some("NaN"),
            Some(Seq(
                spl.Field("host"),
                spl.Field("port"),
                spl.Field("ip")
            ))),
            (_, tree) => {
                FillNullShim("NaN", Set("host", "port", "ip"), tree)
            }
        )
    }

    test("min(bar)") {
        check(spl.SearchCommand(
            spl.Call("min", Seq(
                spl.Field("bar")
            ))),
            (_, tree) => {
                Filter(
                    Min(
                        UnresolvedAttribute("bar")
                    ),
                    tree)
            }
        )
    }

    test("max(bar)") {
        check(spl.SearchCommand(
            spl.Call("max", Seq(
                spl.Field("bar")
            ))),
            (_, tree) => {
                Filter(
                    Max(
                        UnresolvedAttribute("bar")
                    ),
                    tree)
            }
        )
    }

    test("round(x)") {
        check(spl.SearchCommand(
            spl.Call("round", Seq(
                spl.Field("x")
            ))),
            (_, tree) => {
                Filter(
                    Round(
                        UnresolvedAttribute("x"),
                        Literal(0)
                    ),
                    tree)
            }
        )
    }

    test("round(x, 2)") {
        check(spl.SearchCommand(
            spl.Call("round", Seq(
                spl.Field("x"),
                spl.IntValue(2),
            ))),
            (_, tree) => {
                Filter(
                    Round(
                        UnresolvedAttribute("x"),
                        Literal(2)
                    ),
                    tree)
            }
        )
    }

    test("round(min(x))") {
        check(spl.SearchCommand(
            spl.Call("round", Seq(
                spl.Call("min", Seq(
                    spl.Field("x")
                )),
            ))),
            (_, tree) => {
                Filter(
                    Round(
                        Min(
                            UnresolvedAttribute("x")
                        ),
                        Literal(0)
                    ),
                    tree)
            }
        )
    }

    test("equal wildcards should convert to LIKE") {
        check(spl.SearchCommand(
            spl.Binary(
                spl.Field("a"),
                spl.Equals,
                spl.Wildcard("foo*"))),
            (_, tree) =>
                Filter(
                    Like(
                        UnresolvedAttribute("a"),
                        Literal.create("foo%"), '\\'),
                    tree)
        )
    }

    test("not equal wildcards should convert to LIKE") {
        check(spl.SearchCommand(
            spl.Binary(
                spl.Field("a"),
                spl.NotEquals,
                spl.Wildcard("foo*"))),
            (_, tree) =>
                Filter(
                    Not(
                        Like(
                            UnresolvedAttribute("a"),
                            Literal.create("foo%"), '\\')),
                    tree)
        )
    }

    private def check(command: spl.Command,
              callback: (spl.Command, LogicalPlan) => LogicalPlan
              ): Unit = this.synchronized {
        val pipeline = spl.Pipeline(Seq(command))
        val actualPlan: LogicalPlan = SplToCatalyst.pipeline(new LogicalContext(), pipeline)
        val expectedPlan = pipeline.commands.foldLeft(
            UnresolvedRelation(Seq("main")).asInstanceOf[LogicalPlan]) {
            (tree, cmd) => callback(cmd, tree)
        }
        comparePlans(actualPlan, expectedPlan, checkAnalysis = false)
    }
}

