package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{Length, Substring, _}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, PlanTestBase, UsingJoin}
import org.apache.spark.sql.types.DoubleType
import org.scalatest.funsuite.AnyFunSuite

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

    test("FieldsCommand should generate a Project") {
        check(spl.FieldsCommand(
            removeFields = false,
            Seq(
                spl.Field("colA"),
                spl.Field("colB"))
            ),
        (_, tree) =>
            Project(Seq(
                UnresolvedAttribute("colA"),
                UnresolvedAttribute("colB"),
            ), tree))
    }

    test("FieldsCommand should generate another Project 3 columns") {
        check(spl.FieldsCommand(
            removeFields = false,
            Seq(
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

    test("FieldsCommand should generate another Project with 2 columns") {
        check(spl.FieldsCommand(
            removeFields = false,
            Seq(
                spl.Field("colA"),
                spl.Field("colB"))),
        (_, tree) =>
            Project(Seq(
                UnresolvedAttribute("colA"),
                UnresolvedAttribute("colB"),
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
                Seq(
                    UnresolvedAttribute("host"),
                    Alias(
                        AggregateExpression(
                            Count(Seq(Literal.create(1))),
                            Complete, isDistinct = false
                        ), "count")()),
                tree))
    }

    test("sum(connection_time)") {
        check(spl.StatsCommand(
            Map(),
            Seq(spl.Call("sum", Seq(spl.Field("connection_time")))),
            Seq(spl.Field("host"))),
            (_, tree) =>
                Aggregate(
                    Seq(UnresolvedAttribute("host")),
                    Seq(
                        UnresolvedAttribute("host"),
                        Alias(Sum(UnresolvedAttribute("connection_time")), "sum")()),
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
                        Seq(
                            UnresolvedAttribute("host"),
                            Alias(
                                AggregateExpression(
                                    Count(Seq(Literal.create(1))),
                                    Complete, isDistinct = false
                                ), "count")()
                        ),
                        tree)))
    }

    test("EvalCommand to check conditions") {
        check(spl.EvalCommand(
            Seq(
                (spl.Field("a_eq_b"),
                  spl.Call("if",
                      Seq(spl.Binary(spl.Field("a"), spl.Equals, spl.StrValue("b")),
                          spl.IntValue(1),
                          spl.IntValue(0)
                      )
                  )
                )
            )
        ),
            (_, tree) =>
                Project(
                    Seq(Alias(
                        If(
                            EqualTo(UnresolvedAttribute("a"), Literal("b")),
                            Literal(1),
                            Literal(0)
                        ),
                        "a_eq_b"
                    )()
                    )
                    , tree)
        )
    }

    test("EvalCommand to check mvcount function") {
        check(spl.EvalCommand(Seq(
            (spl.Field("count"),
              spl.Call("mvcount",
                  Seq(spl.Field("mvfield"))
              )
            )
        )
        ),
            (_, tree) =>
                Project(Seq(
                    Alias(
                        Size(UnresolvedAttribute("mvfield")),
                        "count"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check mvindex function w/ stop index") {
        check(spl.EvalCommand(Seq(
            (spl.Field("mvsubset"),
              spl.Call("mvindex",
                  Seq(
                      spl.Field("mvfield"),
                      spl.IntValue(0),
                      spl.IntValue(1)
                  )
              )
            )
        )
        ),
            (_, tree) =>
                Project(Seq(
                    Alias(
                        Slice(UnresolvedAttribute("mvfield"), Literal(1), Literal(2)),
                        "mvsubset"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check mvindex function w/o stop index") {
        check(spl.EvalCommand(Seq(
            (spl.Field("mvsubset"),
              spl.Call("mvindex",
                  Seq(
                      spl.Field("mvfield"),
                      spl.IntValue(0),
                  )
              )
            )
        )
        ),
            (_, tree) =>
                Project(Seq(
                    Alias(
                        Slice(UnresolvedAttribute("mvfield"), Literal(1), Literal(1)),
                        "mvsubset"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check mvindex function w/ neg start and stop index") {
        check(spl.EvalCommand(Seq(
            (spl.Field("mvsubset"),
              spl.Call("mvindex",
                  Seq(
                      spl.Field("mvfield"),
                      spl.IntValue(-3),
                      spl.IntValue(-2),
                  )
              )
            )
        )
        ),
            (_, tree) =>
                Project(Seq(
                    Alias(
                        Slice(UnresolvedAttribute("mvfield"), Literal(-3), Literal(2)),
                        "mvsubset"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check mvindex function w/ neg start index") {
        check(spl.EvalCommand(Seq(
            (spl.Field("mvsubset"),
              spl.Call("mvindex",
                  Seq(
                      spl.Field("mvfield"),
                      spl.IntValue(-3)
                  )
              )
            )
        )
        ),
            (_, tree) =>
                Project(Seq(
                    Alias(
                        Slice(UnresolvedAttribute("mvfield"), Literal(-3), Literal(1)),
                        "mvsubset"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check mvappend function") {
        check(spl.EvalCommand(Seq(
            (spl.Field("merged_arrays"),
              spl.Call("mvappend",
                  Seq(
                      spl.Field("mvfieldA"),
                      spl.Field("mvfieldB"),
                      spl.Field("mvfieldC")
                  )
              )
            )
        )
        ),
            (_, tree) =>
                Project(Seq(
                    Alias(
                        Concat(Seq(UnresolvedAttribute("mvfieldA"),UnresolvedAttribute("mvfieldB"), UnresolvedAttribute("mvfieldC"))),
                        "merged_arrays"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check mvfilter function") {
        check(spl.EvalCommand(Seq(
            (spl.Field("filtered_array"),
              spl.Call("mvfilter",
                  Seq(
                      spl.Binary(spl.Field("method"),
                      spl.NotEquals,
                      spl.StrValue("GET"))
                  )
              )
            )
        )
        ),
            (_, tree) =>
                Project(Seq(
                    Alias(
                        ArrayFilter(UnresolvedAttribute("method"),
                            LambdaFunction(Not(EqualTo(UnresolvedNamedLambdaVariable(Seq("method")), Literal("GET"))),Seq(UnresolvedNamedLambdaVariable(Seq("method"))))),
                        "filtered_array"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check mvfilter function with complex expr") {
        check(spl.EvalCommand(Seq(
            (spl.Field("filtered_array"),
              spl.Call("mvfilter",
                  Seq(
                      spl.Binary(spl.Binary(spl.Field("method"),
                          spl.NotEquals,
                          spl.StrValue("GET")),
                          spl.Or,
                          spl.Binary(spl.Field("method"),
                              spl.NotEquals,
                              spl.StrValue("DELETE")))
                  )
              )
            )
        )
        ),
            (_, tree) =>
                Project(Seq(
                    Alias(
                        ArrayFilter(UnresolvedAttribute("method"),
                            LambdaFunction(Or(
                                Not(EqualTo(UnresolvedNamedLambdaVariable(Seq("method")), Literal("GET"))),
                                Not(EqualTo(UnresolvedNamedLambdaVariable(Seq("method")), Literal("DELETE"))))
                                ,Seq(UnresolvedNamedLambdaVariable(Seq("method")))
                            )
                        ),
                        "filtered_array"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check mvfilter function with nested fct") {
        check(spl.EvalCommand(Seq(
            (spl.Field("filtered_array"),
              spl.Call("mvfilter",
                  Seq(
                      spl.Binary(
                          spl.Binary(spl.Call("len", Seq(spl.Field("email"))),
                              spl.GreaterThan,
                              spl.IntValue(5)),
                          spl.And,
                          spl.Binary(spl.Call("len", Seq(spl.Field("email"))),
                              spl.LessThan,
                              spl.IntValue(10)))
                  )
              )
            )
        )
        ),
            (_, tree) =>
                Project(Seq(
                    Alias(
                        ArrayFilter(UnresolvedAttribute("email"),
                            LambdaFunction(And(
                                GreaterThan(Length(UnresolvedNamedLambdaVariable(Seq("email"))), Literal(5)),
                                LessThan(Length(UnresolvedNamedLambdaVariable(Seq("email"))), Literal(10)))
                                ,Seq(UnresolvedNamedLambdaVariable(Seq("email")))
                            )
                        ),
                        "filtered_array"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check coalesce functionality") {
        check(spl.EvalCommand(
            Seq(
                (spl.Field("coalesced"),
                  spl.Call("coalesce",
                      Seq(spl.Field("a"),
                          spl.Field("b"),
                          spl.Field("c")
                      )
                  )
                )
            )
        ),
            (_, tree) =>
                Project(
                    Seq(Alias(
                        Coalesce(
                            Seq(
                                UnresolvedAttribute("a"),
                                UnresolvedAttribute("b"),
                                UnresolvedAttribute("c")
                            )
                        ),
                        "coalesced"
                    )()
                    )
                    , tree)
        )
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
            ), Project(
                Seq(
                    UnresolvedAttribute("_raw")
                ), tree)
            )),
            injectOutput = Seq(
                UnresolvedAttribute("_raw")
            ))
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
                Alias(Column("colNameA").expr, "colARenamed")()
            ), tree)
        , injectOutput = Seq(
            UnresolvedAttribute("colNameA")
        ))
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
                    Alias(Column("colNameA").expr, "colARenamed")(),
                    Alias(Column("colNameB").expr, "colBRenamed")()
                ), tree)
        , injectOutput = Seq(
            UnresolvedAttribute("colNameA"),
            UnresolvedAttribute("colNameB"))
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
        check(spl.ReturnCommand(
            spl.IntValue(10),
            Seq(spl.Field("ip"),
                spl.Field("host"),
                spl.Field("port"))),
        (_, tree) => Limit(Literal(10), Project(Seq(
            UnresolvedAttribute("ip"),
            UnresolvedAttribute("host"),
            UnresolvedAttribute("port"))
        , tree))
        )
    }

    test("return 10 ip $env $test") {
        check(spl.ReturnCommand(
            spl.IntValue(10),
            Seq(spl.Field("env"),
                spl.Field("test"))),
            (_, tree) => Limit(Literal(10), Project(Seq(
                UnresolvedAttribute("env"),
                UnresolvedAttribute("test"))
            , tree))
        )
    }

    test("return 20 a=ip b=host c=port") {
        check(spl.ReturnCommand(
            spl.IntValue(20),
            Seq(
                spl.Alias(spl.Field("ip"), "a"),
                spl.Alias(spl.Field("host"), "b"),
                spl.Alias(spl.Field("port"), "c")
            )),
            (_, tree) => Limit(Literal(20), Project(Seq(
                Alias(UnresolvedAttribute("ip"), "a")(),
                Alias(UnresolvedAttribute("host"), "b")(),
                Alias(UnresolvedAttribute("port"), "c")())
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
                    AggregateExpression(
                        Min(UnresolvedAttribute("bar")),
                        Complete,
                        isDistinct = false
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
                    AggregateExpression(
                        Max(UnresolvedAttribute("bar")),
                        Complete,
                        isDistinct = false
                    ),
                    tree)
            }
        )
    }
    test("length(bar)") {
        check(spl.SearchCommand(
            spl.Call("len", Seq(
                spl.Field("bar")
            ))),
            (_, tree) => {
                Filter(
                    Length(
                        UnresolvedAttribute("bar")
                    ),
                    tree)
            }
        )
    }

    test("substr(foobar,4,3)") {
        check(spl.SearchCommand(
            spl.Call("substr", Seq(
                spl.Field("foobar"),
                spl.IntValue(4),
                spl.IntValue(3)
            ))),
            (_, tree) => {
                Filter(
                    Substring(
                        UnresolvedAttribute("foobar"),
                        Literal(4),
                        Literal(3)
                    ),
                    tree)
            }
        )
    }

    test("substr(foobar,4)") {
        check(spl.SearchCommand(
            spl.Call("substr", Seq(
                spl.Field("foobar"),
                spl.IntValue(4)
            ))),
            (_, tree) => {
                Filter(
                    Substring(
                        UnresolvedAttribute("foobar"),
                        Literal(4),
                        Literal(Integer.MAX_VALUE)
                    ),
                    tree)
            }
        )
    }

    test("substr(foobar,-3)") {
        check(spl.SearchCommand(
            spl.Call("substr", Seq(
                spl.Field("foobar"),
                spl.IntValue(-3)
            ))),
            (_, tree) => {
                Filter(
                    Substring(
                        UnresolvedAttribute("foobar"),
                        Literal(-3),
                        Literal(Integer.MAX_VALUE)
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
                        AggregateExpression(
                            Min(UnresolvedAttribute("x")),
                            Complete,
                            isDistinct = false
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

    test("eventstats max(colA) AS maxA by colC") {
        check(spl.EventStatsCommand(
            Map(),
            Seq(
                spl.Alias(
                    spl.Call("max", Seq(spl.Field("colA"))),
                    "maxA"
                ),
            ),
            Seq(spl.Field("colC"))
        ),
            (_, tree) => Project(Seq(
                Alias(
                    WindowExpression(
                        AggregateExpression(Max(UnresolvedAttribute("colA")), Complete, isDistinct = false),
                        WindowSpecDefinition(
                            Seq(UnresolvedAttribute("colC")),
                            Seq(SortOrder(UnresolvedAttribute("colC"), Ascending)),
                            UnspecifiedFrame
                        )
                    ), "maxA")()
            ), tree))
    }

    test("dedup host") {
        check(spl.DedupCommand(
            1,
            Seq(spl.Field("host")),
            keepEvents = false,
            keepEmpty = false,
            consecutive = false,
            spl.SortCommand(Seq((Some("+"), spl.Field("_no"))))
        ),
        (_, tree) => {
            Project(
                Seq(
                    UnresolvedAttribute("host")
                ),
                Filter(
                    LessThanOrEqual(UnresolvedAttribute("_rn"), Literal(1)),
                    Project(Seq(
                        UnresolvedAttribute("host"),
                        Alias(MonotonicallyIncreasingID(), "_no")(),
                        Alias(WindowExpression(
                            RowNumber(),
                            WindowSpecDefinition(
                                Seq(UnresolvedAttribute("host")),
                                Seq(SortOrder(UnresolvedAttribute("_no"), Ascending)),
                                UnspecifiedFrame
                            )
                        ), "_rn")()
                    ), Project(Seq(
                        UnresolvedAttribute("host"),
                        Alias(MonotonicallyIncreasingID(), "_no")()
                    ), tree))
                )
            )
        }, injectOutput = Seq(
            UnresolvedAttribute("host"))
        )
    }

    test("dedup 10 keepevents=true ip port sortby +host -ip") {
        check(spl.DedupCommand(
            10,
            Seq(
                spl.Field("ip"),
                spl.Field("port")
            ),
            keepEvents = true,
            keepEmpty = false,
            consecutive = false,
            spl.SortCommand(Seq(
                (Some("+"), spl.Field("host")),
                (Some("-"), spl.Field("ip"))))
        ),
        (_, tree) => {
            Project(
                Seq(
                    UnresolvedAttribute("host"),
                    UnresolvedAttribute("ip"),
                    UnresolvedAttribute("port"),
                ),
                Filter(
                    LessThanOrEqual(UnresolvedAttribute("_rn"), Literal(10)),
                    Project(Seq(
                        UnresolvedAttribute("host"),
                        UnresolvedAttribute("ip"),
                        UnresolvedAttribute("port"),
                        Alias(MonotonicallyIncreasingID(), "_no")(),
                        Alias(WindowExpression(
                            RowNumber(),
                            WindowSpecDefinition(
                                Seq(UnresolvedAttribute("ip"),
                                    UnresolvedAttribute("port")),
                                Seq(SortOrder(UnresolvedAttribute("host"), Ascending),
                                    SortOrder(UnresolvedAttribute("ip"), Descending)),
                                UnspecifiedFrame
                            )
                        ), "_rn")()
                    ), Project(Seq(
                        UnresolvedAttribute("host"),
                        UnresolvedAttribute("ip"),
                        UnresolvedAttribute("port"),
                        Alias(MonotonicallyIncreasingID(), "_no")()
                    ), tree))
                )
            )
        }, injectOutput = Seq(
            UnresolvedAttribute("host"),
            UnresolvedAttribute("ip"),
            UnresolvedAttribute("port")
        ))
    }

    test("inputlookup append=t strict=f max=20 main where a > 10") {
        check(spl.InputLookup(
            append = true,
            strict = false,
            start = 0,
            max = 20,
            tableName = "main",
            Some(
                spl.Binary(
                    spl.Field("a"),
                    spl.GreaterThan,
                    spl.IntValue(10)
                )
            )
        ),
        (_, tree) => {
            Limit(
                Literal(20),
                Filter(GreaterThan(
                    UnresolvedAttribute("a"),
                    Literal(10)
                ), tree)
            )
        })
    }

    test("format maxresults=12") {
        check(spl.FormatCommand(
            mvSep = "||",
            maxResults = 12,
            rowPrefix = "(",
            colPrefix =  "(",
            colSep = "AND",
            colEnd = ")",
            rowSep = "OR",
            rowEnd = ")"
        ),
        (_, tree) => {
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
                                    ): _*))),
                                Complete,
                                isDistinct = false
                            ),
                            Literal(" OR "),
                            None
                        ), "search")()
                ),
                Limit(Literal(12), tree)
            )
        }, injectOutput = Seq(
            UnresolvedAttribute("a"),
            UnresolvedAttribute("b")
        ))
    }

    test("mvcombine host") {
        check(spl.MvCombineCommand(
            None,
            spl.Field("host")
        ),
            (_, tree) => {
                Aggregate(
                    Seq(
                        UnresolvedAttribute("ip"),
                        UnresolvedAttribute("port")
                    ),
                    Seq(
                        UnresolvedAttribute("ip"),
                        UnresolvedAttribute("port"),
                        Alias(
                            AggregateExpression(
                                CollectList(UnresolvedAttribute("host")),
                                Complete,
                                isDistinct = false
                            ), "host")()
                    )
                    , tree)
            }, injectOutput = Seq(
                UnresolvedAttribute("host"),
                UnresolvedAttribute("ip"),
                UnresolvedAttribute("port")
            ))
    }

    test("mvcombine delim=\",\" host") {
        check(spl.MvCombineCommand(
            Some(","),
            spl.Field("host")
        ),
        (_, tree) => {
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
                )
            , tree)
        }, injectOutput = Seq(
            UnresolvedAttribute("host"),
            UnresolvedAttribute("ip"),
            UnresolvedAttribute("port")
        ))
    }

    test("bin spans") {
        check(spl.BinCommand(
            spl.Alias(spl.Field("ts"), "time_bin"),
            span = Some(spl.TimeSpan(1, "hours"))
        ),
        (_, tree) => Project(Seq(
                Alias(UnresolvedAttribute("time_bin.start"), "time_bin")()
            ), Project(Seq(
                Alias(TimeWindow(
                    UnresolvedAttribute("ts"),
                    3600000000L,
                    3600000000L,
                    0), "time_bin")()
            ), tree)))
    }

    private def check(command: spl.Command,
                      callback: (spl.Command, LogicalPlan) => LogicalPlan,
                      injectOutput: Seq[NamedExpression] = Seq()): Unit = this.synchronized {
        val pipeline = spl.Pipeline(Seq(command))
        val actualPlan: LogicalPlan = SplToCatalyst.pipeline(new LogicalContext(output = injectOutput), pipeline)
        val expectedPlan = pipeline.commands.foldLeft(
            UnresolvedRelation(Seq("main")).asInstanceOf[LogicalPlan]) {
            (tree, cmd) => callback(cmd, tree)
        }
        comparePlans(actualPlan, expectedPlan, checkAnalysis = false)
    }
}

