package org.apache.spark.sql

import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import spl.{Call, FieldsCommand, HeadCommand, IntValue, Pipeline, SortCommand, Value}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedRegex, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Limit, LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Cast, Descending, GreaterThan, Literal, SortOrder}
import org.apache.spark.sql.types.{DoubleType, StringType}

class SplToCatalystTestSuite extends AnyFunSuite {

    private val mySqlToCatalyst = new SplToCatalyst

    /**
     *
     * @param command
     * @param callback
     * @return
     */
    def assertPlan(command: spl.Command, callback: (spl.Command, LogicalPlan) => LogicalPlan): Assertion = {
        val myPipeline = Pipeline(Seq(command))
        val myPlanToTest: LogicalPlan = mySqlToCatalyst.process(myPipeline)

        val expectedPlan = myPipeline.commands.foldLeft(
            UnresolvedRelation(Seq("x")).asInstanceOf[LogicalPlan]) {
            (tree, cmd) => callback(cmd, tree)
        }
        assert(myPlanToTest == expectedPlan)
    }

    test("HeadCommand should generate a Limit") {

        assertPlan(
            HeadCommand(IntValue(10), spl.Bool(false), spl.Bool(false)),
            (_, tree) => Limit(Literal(10), tree))
    }

    test("HeadCommand should generate a Filter") {
        assertPlan(
            HeadCommand(spl.Binary(Value("count"), spl.GreaterThan, spl.IntValue(10))),
            (_, tree) => Filter(GreaterThan(UnresolvedAttribute("count"), Literal(10)), tree)
        )
    }

    test("SortCommand command should generate a Sort") {
        assertPlan(
            SortCommand(Seq(
                Tuple2(Some("+"), Value("A")),
                Tuple2(Some("-"), Value("B")),
                Tuple2(Some("+"), Call("num", Seq(Value("C")))))),
            (_, tree) => Sort(Seq(
                SortOrder(Literal("A"), Ascending),
                SortOrder(Literal("B"), Descending),
                SortOrder(Cast(Literal("C"), DoubleType), Ascending)
            ), global =  true, tree)
        )
    }

    test("SortCommand command should generate another Sort") {
        assertPlan(
            SortCommand(Seq(Tuple2(None, Value("A")))),
            (_, tree) => Sort(Seq(
                SortOrder(Literal("A"), Ascending)
            ), global =  true, tree)
        )
    }

    test("FieldsCommand should generate a Project with UnresolvedAlias") {
        assertPlan(
            FieldsCommand(None, Seq(Value("colA"), Value("colB"))),
            (_, tree) => Project(Seq(
                Column("colA").named,
                Column("colB").named,
            ), tree)
        )
    }


    test("FieldsCommand should generate another Project with with UnresolvedAlias") {
        assertPlan(
            FieldsCommand(Some("+"), Seq(Value("colA"), Value("colB"), Value("colC"))),
            (_, tree) => Project(Seq(
                Column("colA").named,
                Column("colB").named,
                Column("colC").named,
            ), tree)
        )
    }

    test("FieldsCommand should generate a Project with UnresolvedRegex") {
        assertPlan(
            FieldsCommand(Some("-"), Seq(Value("colA"), Value("colB"))),
            (_, tree) => Project(Seq(
                UnresolvedRegex("^(?!$colA|colB).*$", None, caseSensitive = false)
            ), tree)
        )
    }

}
