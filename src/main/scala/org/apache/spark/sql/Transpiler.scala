package org.apache.spark.sql

import fastparse.{Parsed, parse}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object Transpiler {
  private def parsePipeline(search: String): spl.Pipeline =
    parse(search, spl.SplParser.pipeline(_), verboseFailures = true) match {
      case Parsed.Success(value, _) => value
      case f: Parsed.Failure =>
        throw new AssertionError(f.trace().longMsg)
    }

  /** Converts SPL AST to Databricks Runtime internal Logical Execution Plan */
  private def logicalPlan(search: String): LogicalPlan = {
    val tsc = new SplToCatalyst()
    tsc.process(parsePipeline(search))
  }

  /** Executes SPL query on Databricks Runtime */
  def toDataFrame(spark: SparkSession, search: String): DataFrame =
    Dataset.ofRows(spark, logicalPlan(search))

  /** Generates PySpark code out of SPL search */
  def toPython(search: String): String = {
    val pg = new PythonGenerator()
    "(" + pg.fromPlan(logicalPlan(search)) + ")\n"
  }
}
