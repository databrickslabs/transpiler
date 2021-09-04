package org.apache.spark.sql

import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import spl.{HeadCommand, IntValue, Pipeline, Value}
import org.apache.spark.sql.catalyst.expressions.{GreaterThan, Literal}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Limit, LogicalPlan}

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

}
