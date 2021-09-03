package org.apache.spark.sql
import org.scalatest.funsuite.AnyFunSuite
import spl.{HeadCommand, IntValue, Pipeline, Value}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Limit, LogicalPlan}

class SplToCatalystTestSuite extends AnyFunSuite {

    private val mySqlToCatalyst = new SplToCatalyst

    test("HeadCommand should generate a Limit") {
        val myPipeline = Pipeline(Seq(HeadCommand(IntValue(10), None, None)))
        val plan: LogicalPlan = mySqlToCatalyst.process(myPipeline)

        val expectedPlan = myPipeline.commands.foldLeft(
            UnresolvedRelation(Seq("x")).asInstanceOf[LogicalPlan]) { (tree, _) =>
                Limit(Literal(10), tree)
        }
        assert(expectedPlan == plan)
    }

    test("HeadCommand should generate a Filter") {
        val myPipeline = Pipeline(Seq(HeadCommand((Value("count>10")), None, None)))
        val plan: LogicalPlan = mySqlToCatalyst.process(myPipeline)

        val expectedPlan = myPipeline.commands.foldLeft(
            UnresolvedRelation(Seq("x")).asInstanceOf[LogicalPlan]) { (tree, _) =>
                Filter(Literal("count>10"), tree)
        }
        assert(expectedPlan == plan)
    }

}
