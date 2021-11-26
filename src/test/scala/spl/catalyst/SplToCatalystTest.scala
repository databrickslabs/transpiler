package spl.catalyst

import org.apache.spark.sql.{CidrMatch, FillNullShim}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{Length, Substring, _}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, PlanTestBase, UsingJoin}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.scalatest.funsuite.AnyFunSuite
import spl.ast

class SplToCatalystTest extends AnyFunSuite with PlanTestBase {
    test("HeadCommand should generate a Limit") {
        check(ast.HeadCommand(
            ast.IntValue(10),
            ast.Bool(false),
            ast.Bool(false)),
            (_, tree) =>
                Limit(Literal(10), tree))
    }

    test("HeadCommand should generate a Filter") {
        check(ast.HeadCommand(
            ast.Binary(
                ast.Field("count"),
                ast.GreaterThan,
                ast.IntValue(10))),
            (_, tree) =>
                Filter(
                    GreaterThan(
                        UnresolvedAttribute("count"),
                        Literal(10)),
                    tree))
    }

    test("SortCommand command should generate a Sort") {
        check(ast.SortCommand(Seq(
            Tuple2(Some("+"), ast.Field("A")),
            Tuple2(Some("-"), ast.Field("B")),
            Tuple2(Some("+"), ast.Call("num", Seq(ast.Field("C")))))),
            (_, tree) =>
                Sort(Seq(
                    SortOrder(UnresolvedAttribute("A"), Ascending),
                    SortOrder(UnresolvedAttribute("B"), Descending),
                    SortOrder(Cast(UnresolvedAttribute("C"), DoubleType), Ascending)
                ), global = true, tree))
    }

    test("SortCommand command should generate another Sort") {
        check(ast.SortCommand(Seq(
            Tuple2(None, ast.Field("A")))),
            (_, tree) =>
                Sort(Seq(
                    SortOrder(UnresolvedAttribute("A"), Ascending)
                ), global = true, tree))
    }

    test("FieldsCommand should generate a Project") {
        check(ast.FieldsCommand(removeFields = false,
            Seq(ast.Field("colA"), ast.Field("colB"))),
        (_, tree) =>
            Project(Seq(
                UnresolvedAttribute("colA"),
                UnresolvedAttribute("colB")
            ), tree))
    }

    test("FieldsCommand should generate another Project 3 columns") {
        check(ast.FieldsCommand(
            removeFields = false,
            Seq(
                ast.Field("colA"),
                ast.Field("colB"),
                ast.Field("colC"))),
            (_, tree) =>
                Project(Seq(
                    UnresolvedAttribute("colA"),
                    UnresolvedAttribute("colB"),
                    UnresolvedAttribute("colC")
                ), tree))
    }

    test("FieldsCommand should generate another Project with 2 columns") {
        check(ast.FieldsCommand(
            removeFields = false,
            Seq(
                ast.Field("colA"),
                ast.Field("colB"))),
            (_, tree) =>
                Project(Seq(
                    UnresolvedAttribute("colA"),
                    UnresolvedAttribute("colB")
                ), tree))
    }

    test("StatsCommand to Aggregate") {
        check(ast.StatsCommand(
            partitions = 1,
            allNum = false,
            delim = " ",
            Seq(ast.Call("count", Seq())),
            Seq(ast.Field("host"))),
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
        check(ast.StatsCommand(
            partitions = 1,
            allNum = false,
            delim = " ",
            Seq(ast.Call("sum", Seq(ast.Field("connection_time")))),
            Seq(ast.Field("host"))),
            (_, tree) =>
                Aggregate(
                    Seq(UnresolvedAttribute("host")),
                    Seq(
                        UnresolvedAttribute("host"),
                        Alias(Sum(UnresolvedAttribute("connection_time")), "sum")()),
                    tree))
    }

    test("StatsCommand to Deduplicate Aggregate") {
        check(ast.StatsCommand(
            partitions = 1,
            allNum = false,
            delim = " ",
            Seq(ast.Call("count", Seq())),
            Seq(ast.Field("host")),
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
        check(ast.EvalCommand(
            Seq(
                (ast.Field("a_eq_b"),
                  ast.Call("if",
                      Seq(ast.Binary(ast.Field("a"), ast.Equals, ast.StrValue("b")),
                          ast.IntValue(1),
                          ast.IntValue(0)
                      )
                  )
                )
            )
        ),
        (_, tree) =>
            Project(
                Seq(Alias(
                    CaseWhen(
                        Seq((EqualTo(UnresolvedAttribute("a"), Literal("b")), Literal(1))),
                        Literal(0)
                    ),
                    "a_eq_b"
                )()), tree)
        )
    }

    test("EvalCommand to check mvcount function") {
        check(ast.EvalCommand(Seq(
            (ast.Field("count"),
              ast.Call("mvcount",
                  Seq(ast.Field("mvfield"))
              )
            )
        )),
        (_, tree) =>
            Project(Seq(
                Alias(
                    Size(UnresolvedAttribute("mvfield")),
                    "count"
                )()
            ), tree))
    }

    test("EvalCommand to check mvindex function w/ stop index") {
        check(ast.EvalCommand(Seq(
            (ast.Field("mvsubset"),
              ast.Call("mvindex",
                  Seq(ast.Field("mvfield"), ast.IntValue(0), ast.IntValue(1))
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
            ), tree))
    }

    test("EvalCommand to check mvindex function w/o stop index") {
        check(ast.EvalCommand(Seq(
            (ast.Field("mvsubset"),
              ast.Call("mvindex",
                  Seq(ast.Field("mvfield"), ast.IntValue(0))
              )
            )
        )),
        (_, tree) =>
            Project(Seq(
                Alias(
                    Slice(UnresolvedAttribute("mvfield"), Literal(1), Literal(1)),
                    "mvsubset")()
            ), tree))
    }

    test("EvalCommand to check mvindex function w/ neg start and stop index") {
        check(ast.EvalCommand(Seq(
            (ast.Field("mvsubset"),
              ast.Call("mvindex",
                  Seq(ast.Field("mvfield"), ast.IntValue(-3), ast.IntValue(-2))
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
        check(ast.EvalCommand(Seq(
            (ast.Field("mvsubset"),
              ast.Call("mvindex",
                  Seq(ast.Field("mvfield"), ast.IntValue(-3))
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
        check(ast.EvalCommand(Seq(
            (ast.Field("merged_arrays"),
              ast.Call("mvappend",
                  Seq(
                      ast.Field("mvfieldA"),
                      ast.Field("mvfieldB"),
                      ast.Field("mvfieldC")
                  )
              )
            )
        )
        ),
            (_, tree) =>
                Project(Seq(
                    Alias(
                        Concat(Seq(
                            UnresolvedAttribute("mvfieldA"),
                            UnresolvedAttribute("mvfieldB"),
                            UnresolvedAttribute("mvfieldC")
                        )),
                        "merged_arrays"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check mvfilter function") {
        check(ast.EvalCommand(Seq(
            (ast.Field("filtered_array"),
              ast.Call("mvfilter",
                  Seq(
                      ast.Binary(ast.Field("method"),
                          ast.NotEquals,
                          ast.StrValue("GET"))
                  )
              )
            )
        )
        ),
            (_, tree) =>
                Project(Seq(
                    Alias(
                        ArrayFilter(UnresolvedAttribute("method"),
                            LambdaFunction(Not(EqualTo(
                                UnresolvedNamedLambdaVariable(Seq("method")),
                                Literal("GET"))),
                                Seq(UnresolvedNamedLambdaVariable(Seq("method"))))),
                        "filtered_array"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check mvfilter function with complex expr") {
        check(ast.EvalCommand(Seq(
            (ast.Field("filtered_array"),
              ast.Call("mvfilter",
                  Seq(
                      ast.Binary(ast.Binary(ast.Field("method"),
                          ast.NotEquals,
                          ast.StrValue("GET")),
                          ast.Or,
                          ast.Binary(ast.Field("method"),
                              ast.NotEquals,
                              ast.StrValue("DELETE")))
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
                                Not(EqualTo(
                                    UnresolvedNamedLambdaVariable(Seq("method")),
                                    Literal("GET"))),
                                Not(EqualTo(
                                    UnresolvedNamedLambdaVariable(Seq("method")),
                                    Literal("DELETE")))),
                                Seq(UnresolvedNamedLambdaVariable(Seq("method")))
                            )
                        ),
                        "filtered_array"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check mvfilter function with nested fct") {
        check(ast.EvalCommand(Seq(
            (ast.Field("filtered_array"),
              ast.Call("mvfilter",
                  Seq(
                      ast.Binary(
                          ast.Binary(ast.Call("len", Seq(ast.Field("email"))),
                              ast.GreaterThan,
                              ast.IntValue(5)),
                          ast.And,
                          ast.Binary(ast.Call("len", Seq(ast.Field("email"))),
                              ast.LessThan,
                              ast.IntValue(10)))
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
                                GreaterThan(Length(
                                    UnresolvedNamedLambdaVariable(Seq("email"))),
                                    Literal(5)),
                                LessThan(Length(
                                    UnresolvedNamedLambdaVariable(Seq("email"))),
                                    Literal(10))),
                                Seq(UnresolvedNamedLambdaVariable(Seq("email")))
                            )
                        ),
                        "filtered_array"
                    )()
                )
                    , tree)
        )
    }

    test("EvalCommand to check coalesce functionality") {
        check(ast.EvalCommand(
            Seq(
                (ast.Field("coalesced"),
                  ast.Call("coalesce",
                      Seq(ast.Field("a"),
                          ast.Field("b"),
                          ast.Field("c")
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

    test("EvalCommand to check null functionality") {
        check(ast.EvalCommand(
            Seq(
                (ast.Field("x_no_neg"),
                  ast.Call("if",
                      Seq(ast.Binary(ast.Field("x"),
                          ast.GreaterThan,
                          ast.IntValue(0)),
                          ast.Field("x"),
                          ast.Call("null", Seq())
                      )
                  )
                )
            )
        ),
            (_, tree) =>
                Project(
                    Seq(Alias(
                        CaseWhen(
                            Seq((GreaterThan(UnresolvedAttribute("x"), Literal(0)),
                              UnresolvedAttribute("x"))),
                            Literal(null)
                        ),
                        "x_no_neg"
                    )()
                    )
                    , tree)
        )
    }

    test("EvalCommand to check isnotnull functionality") {
        check(ast.EvalCommand(
            Seq(
                (ast.Field("x_not_null"),
                  ast.Call("if",
                      Seq(ast.Call("isnotnull", Seq(ast.Field("x"))),
                          ast.StrValue("yes"),
                          ast.StrValue("no")
                      )
                  )
                )
            )
        ),
            (_, tree) =>
                Project(
                    Seq(Alias(
                        CaseWhen(Seq(
                            (IsNotNull(UnresolvedAttribute("x")), Literal("yes"))),
                            Literal("no")),
                        "x_not_null")()
                    ), tree)
        )
    }

    test("LookupCommand to Join") {
        check(ast.LookupCommand(
            "dst", Seq(
                ast.Field("a"),
                ast.AliasedField(ast.Field("b"), "c")
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
        check(ast.RexCommand(
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
                        Literal(1)), "from")()
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
        check(ast.RexCommand(
            Some("colNameA"),
            1,
            None,
            Some("sed"),
            // scalastyle:off
            "s/(\\d{4}-){3}/XXXX-XXXX-XXXX-/g"),
            // scalastyle:on
        (_, tree) => UnknownPlanShim(
            "Error in rex: scala.NotImplementedError: rex mode=sed currently not supported!",
            tree))
    }

    test("rename command should generate a Project") {
        check(ast.RenameCommand(Seq(
            ast.Alias(
                ast.Field("colNameA"),
                "colARenamed"))),
            (_, tree) =>
                Project(Seq(
                    Alias(UnresolvedAttribute("colNameA"), "colARenamed")()
                ), tree)
            , injectOutput = Seq(
                UnresolvedAttribute("colNameA")
            ))
    }

    test("rename command should generate another Project") {
        check(ast.RenameCommand(Seq(
            ast.Alias(
                ast.Field("colNameA"),
                "colARenamed"),
            ast.Alias(
                ast.Field("colNameB"),
                "colBRenamed"))),
            (_, tree) =>
                Project(Seq(
                    Alias(UnresolvedAttribute("colNameA"), "colARenamed")(),
                    Alias(UnresolvedAttribute("colNameB"), "colBRenamed")()
                ), tree)
            , injectOutput = Seq(
                UnresolvedAttribute("colNameA"),
                UnresolvedAttribute("colNameB"))
        )
    }


    test("regex command should generate a Filter") {
        check(ast.RegexCommand(
            Some((ast.Field("colNameA"), "!=")),
            "[0-9]{5}(-[0-9]{4})?"),
            (_, tree) =>
                Filter(Not(
                    RLike(
                        UnresolvedAttribute("colNameA"),
                        Literal("[0-9]{5}(-[0-9]{4})?"))
                ), tree)
        )
    }

    test("return 10 ip host port") {
        check(ast.ReturnCommand(
            ast.IntValue(10),
            Seq(ast.Field("ip"),
                ast.Field("host"),
                ast.Field("port"))),
            (_, tree) => Limit(Literal(10), Project(Seq(
                UnresolvedAttribute("ip"),
                UnresolvedAttribute("host"),
                UnresolvedAttribute("port"))
                , tree))
        )
    }

    test("return 10 ip $env $test") {
        check(ast.ReturnCommand(
            ast.IntValue(10),
            Seq(ast.Field("env"),
                ast.Field("test"))),
            (_, tree) => Limit(Literal(10), Project(Seq(
                UnresolvedAttribute("env"),
                UnresolvedAttribute("test"))
                , tree))
        )
    }

    test("return 20 a=ip b=host c=port") {
        check(ast.ReturnCommand(
            ast.IntValue(20),
            Seq(
                ast.Alias(ast.Field("ip"), "a"),
                ast.Alias(ast.Field("host"), "b"),
                ast.Alias(ast.Field("port"), "c")
            )),
            (_, tree) => Limit(Literal(20), Project(Seq(
                Alias(UnresolvedAttribute("ip"), "a")(),
                Alias(UnresolvedAttribute("host"), "b")(),
                Alias(UnresolvedAttribute("port"), "c")())
                , tree))
        )
    }

    test("join product_id [search vendors]") {
        check(ast.JoinCommand(
            joinType = "inner",
            useTime = false,
            earlier = true,
            overwrite = false,
            max = 1,
            Seq(ast.Field("product_id")),
            ast.Pipeline(Seq(
                ast.SearchCommand(ast.Field("vendors"))
            ))),
            (_, tree) => {
                Join(tree,
                    Filter(Literal("vendors"), tree),
                    UsingJoin(Inner, List("product_id")), None, JoinHint.NONE)
            }
        )
    }

    test("join type=left product_id product_name [search vendors]") {
        check(ast.JoinCommand(
            joinType = "left",
            useTime = false,
            earlier = true,
            overwrite = false,
            max = 1,
            Seq(ast.Field("product_id"), ast.Field("product_name")),
            ast.Pipeline(Seq(
                ast.SearchCommand(ast.Field("vendors"))
            ))),
            (_, tree) => {
                Join(tree,
                    Filter(Literal("vendors"), tree),
                    UsingJoin(LeftOuter, List("product_id", "product_name")), None, JoinHint.NONE)
            }
        )
    }

    test("fillnull") {
        check(ast.FillNullCommand(
            Some("0"),
            None
        ),
            (_, tree) => {
                FillNullShim("0", Set.empty[String], tree)
            }
        )
    }

    test("fillnull value=\"NaN\" host port ip") {
        check(ast.FillNullCommand(
            Some("NaN"),
            Some(Seq(
                ast.Field("host"),
                ast.Field("port"),
                ast.Field("ip")
            ))),
            (_, tree) => {
                FillNullShim("NaN", Set("host", "port", "ip"), tree)
            }
        )
    }

    test("min(bar)") {
        check(ast.SearchCommand(
            ast.Call("min", Seq(
                ast.Field("bar")
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
        check(ast.SearchCommand(
            ast.Call("max", Seq(
                ast.Field("bar")
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
        check(ast.SearchCommand(
            ast.Call("len", Seq(
                ast.Field("bar")
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
        check(ast.SearchCommand(
            ast.Call("substr", Seq(
                ast.Field("foobar"),
                ast.IntValue(4),
                ast.IntValue(3)
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
        check(ast.SearchCommand(
            ast.Call("substr", Seq(
                ast.Field("foobar"),
                ast.IntValue(4)
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
        check(ast.SearchCommand(
            ast.Call("substr", Seq(
                ast.Field("foobar"),
                ast.IntValue(-3)
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

    test("cidrmatch(\"10.0.0.0/24\",\"10.0.0.42\")") {
        check(ast.SearchCommand(
            ast.Call("if", Seq(
                ast.Call("cidrmatch", Seq(
                    ast.IPv4CIDR("10.0.0.0/24"),
                    ast.StrValue("10.0.0.42")
                )),
                ast.StrValue("yes"),
                ast.StrValue("no")
            ))),
            (_, tree) => {
                Filter(
                    CaseWhen(
                        Seq((
                          CidrMatch(Literal("10.0.0.0/24"), Literal("10.0.0.42")),
                          Literal("yes")
                        )),
                        Some(Literal("no"))
                    ),
                    tree)
            }
        )
    }

    test("src_ip = 10.0.0.0/24") {
        check(ast.SearchCommand(
            ast.Binary(
                ast.Field("src_ip"),
                ast.Equals,
                ast.IPv4CIDR("10.0.0.0/24")
            )),
            (_, tree) => {
                Filter(
                    CidrMatch(
                        Literal("10.0.0.0/24"),
                        UnresolvedAttribute("src_ip")
                    ),
                    tree)
            }
        )
    }

    test("round(x)") {
        check(ast.SearchCommand(
            ast.Call("round", Seq(
                ast.Field("x")
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
        check(ast.SearchCommand(
            ast.Call("round", Seq(
                ast.Field("x"),
                ast.IntValue(2)
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
        check(ast.SearchCommand(
            ast.Call("round", Seq(
                ast.Call("min", Seq(
                    ast.Field("x")
                ))
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
        check(ast.SearchCommand(
            ast.Binary(
                ast.Field("a"),
                ast.Equals,
                ast.Wildcard("foo*"))),
            (_, tree) =>
                Filter(
                    Like(
                        UnresolvedAttribute("a"),
                        Literal.create("foo%"), '\\'),
                    tree)
        )
    }

    test("not equal wildcards should convert to LIKE") {
        check(ast.SearchCommand(
            ast.Binary(
                ast.Field("a"),
                ast.NotEquals,
                ast.Wildcard("foo*"))),
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
        check(ast.EventStatsCommand(
            allNum = false,
            Seq(
                ast.Alias(
                    ast.Call("max", Seq(ast.Field("colA"))),
                    "maxA"
                )
            ),
            Seq(ast.Field("colC"))
        ),
            (_, tree) => Project(Seq(
                Alias(
                    WindowExpression(
                        AggregateExpression(
                            Max(UnresolvedAttribute("colA")), Complete, isDistinct = false),
                        WindowSpecDefinition(
                            Seq(UnresolvedAttribute("colC")),
                            Seq(SortOrder(UnresolvedAttribute("colC"), Ascending)),
                            UnspecifiedFrame
                        )
                    ), "maxA")()
            ), tree))
    }

    test("streamstats max(colA) AS maxA by colC") {
        check(ast.StreamStatsCommand(
            Seq(
                ast.Alias(
                    ast.Call("max", Seq(ast.Field("colA"))),
                    "maxA"
                )
            ),
            Seq(ast.Field("colC"))
        ),
            (_, tree) => Project(Seq(
                Alias(
                    WindowExpression(
                        AggregateExpression(Max(
                            UnresolvedAttribute("colA")), Complete, isDistinct = false),
                        WindowSpecDefinition(
                            Seq(UnresolvedAttribute("colC")),
                            Seq(SortOrder(UnresolvedAttribute("_time"), Ascending)),
                            SpecifiedWindowFrame(RowFrame, UnboundedPreceding, Literal(0))
                        )
                    ), "maxA")()
            ), tree))
    }

    test("dedup host") {
        check(ast.DedupCommand(
            1,
            Seq(ast.Field("host")),
            keepEvents = false,
            keepEmpty = false,
            consecutive = false,
            ast.SortCommand(Seq((Some("+"), ast.Field("_no"))))
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
        check(ast.DedupCommand(
            10,
            Seq(
                ast.Field("ip"),
                ast.Field("port")
            ),
            keepEvents = true,
            keepEmpty = false,
            consecutive = false,
            ast.SortCommand(Seq(
                (Some("+"), ast.Field("host")),
                (Some("-"), ast.Field("ip"))))
        ),
            (_, tree) => {
                Project(
                    Seq(
                        UnresolvedAttribute("host"),
                        UnresolvedAttribute("ip"),
                        UnresolvedAttribute("port")
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
        check(ast.InputLookup(
            append = true,
            strict = false,
            start = 0,
            max = 20,
            tableName = "main",
            Some(
                ast.Binary(
                    ast.Field("a"),
                    ast.GreaterThan,
                    ast.IntValue(10)
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
        check(ast.FormatCommand(
            mvSep = "||",
            maxResults = 12,
            rowPrefix = "(",
            colPrefix = "(",
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
                                        FormatString(Literal("((a=%s) AND (b=%s))") +: Seq(
                                            UnresolvedAttribute("a"),
                                            UnresolvedAttribute("b")
                                        ): _*)),
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
        check(ast.MvCombineCommand(
            None,
            ast.Field("host")
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
        check(ast.MvCombineCommand(
            Some(","),
            ast.Field("host")
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
                            ), "host")()
                    )
                    , tree)
            }, injectOutput = Seq(
                UnresolvedAttribute("host"),
                UnresolvedAttribute("ip"),
                UnresolvedAttribute("port")
            ))
    }

    test("mvexpand country") {
        check(ast.MvExpandCommand(
            ast.Field("country"),
            None
        ),
            (_, tree) => {
                Project(Seq(
                    Alias(Explode(UnresolvedAttribute("country")),
                        "country")()),
                    tree
                )
            }
        )
    }

    test("bin spans") {
        check(ast.BinCommand(
            ast.Alias(ast.Field("ts"), "time_bin"),
            span = Some(ast.TimeSpan(1, "hours"))
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

    test("makeresults count=10 annotate=t splunk_server_group=group0") {
        check(ast.MakeResults(
            count = 10,
            annotate = true,
            splunkServer = "local",
            splunkServerGroup = "group0"),
            (_, tree) =>
                Project(Seq(
                    UnresolvedAttribute("_raw"),
                    UnresolvedAttribute("_time"),
                    UnresolvedAttribute("host"),
                    UnresolvedAttribute("source"),
                    UnresolvedAttribute("sourcetype"),
                    UnresolvedAttribute("splunk_server"),
                    UnresolvedAttribute("splunk_server_group")),
                    Project(Seq(
                        Alias(Literal(null), "_raw")(),
                        Alias(CurrentTimestamp(), "_time")(),
                        Alias(Literal(null), "host")(),
                        Alias(Literal(null), "source")(),
                        Alias(Literal(null), "sourcetype")(),
                        Alias(Literal("local"), "splunk_server")(),
                        Alias(Literal("group0"), "splunk_server_group")()
                    ), Project(Seq(
                        Alias(Literal(null), "_raw")(),
                        Alias(CurrentTimestamp(), "_time")(),
                        Alias(Literal(null), "host")(),
                        Alias(Literal(null), "source")(),
                        Alias(Literal(null), "sourcetype")(),
                        Alias(Literal("local"), "splunk_server")()
                    ), Project(Seq(
                        Alias(Literal(null), "_raw")(),
                        Alias(CurrentTimestamp(), "_time")(),
                        Alias(Literal(null), "host")(),
                        Alias(Literal(null), "source")(),
                        Alias(Literal(null), "sourcetype")()
                    ), Project(Seq(
                        Alias(Literal(null), "_raw")(),
                        Alias(CurrentTimestamp(), "_time")(),
                        Alias(Literal(null), "host")(),
                        Alias(Literal(null), "source")()
                    ), Project(Seq(
                        Alias(Literal(null), "_raw")(),
                        Alias(CurrentTimestamp(), "_time")(),
                        Alias(Literal(null), "host")()
                    ), Project(Seq(
                        Alias(Literal(null), "_raw")(),
                        Alias(CurrentTimestamp(), "_time")()
                    ), Project(Seq(
                        Alias(Literal(null), "_raw")()
                    ), Range(0, 10, 1, None))))))))
                )
        )
    }

    test("addtotals fieldname=num_total") {
        check(ast.AddTotals(
            Seq(ast.Field("num_men"),
                ast.Field("num_women")),
            row = true,
            col = false,
            "num_total",
            null,
            "Total"
        ),
            (_, tree) => Project(Seq(
                UnresolvedAttribute("num_men"),
                UnresolvedAttribute("num_women"),
                Alias(Add(
                    CaseWhen(Seq((
                      IsNotNull(Cast(UnresolvedAttribute("num_women"), DoubleType)),
                      UnresolvedAttribute("num_women"))), Literal(0.0)),
                    CaseWhen(Seq((
                      IsNotNull(Cast(UnresolvedAttribute("num_men"), DoubleType)),
                      UnresolvedAttribute("num_men"))), Literal(0.0))
                ), "num_total")()
            ), tree), injectOutput = Seq(
                UnresolvedAttribute("num_men"),
                UnresolvedAttribute("num_women"))
        )
    }

    private def check(command: ast.Command,
                      callback: (ast.Command, LogicalPlan) => LogicalPlan,
                      injectOutput: Seq[NamedExpression] = Seq()): Unit = this.synchronized {
        val pipeline = ast.Pipeline(Seq(command))
        val actualPlan: LogicalPlan = SplToCatalyst.pipeline(
            new LogicalContext(output = injectOutput), pipeline)
        val expectedPlan = pipeline.commands.foldLeft(
            UnresolvedRelation(Seq("main")).asInstanceOf[LogicalPlan]) {
            (tree, cmd) => callback(cmd, tree)
        }
        comparePlans(actualPlan, expectedPlan, checkAnalysis = false)
    }
}
