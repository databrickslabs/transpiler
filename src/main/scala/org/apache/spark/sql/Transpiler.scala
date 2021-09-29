package org.apache.spark.sql

import fastparse.{Parsed, parse}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

private class LogicalContext(
   val indexName: String = "main",
   val timeFieldName: String = "_time",
   val rawFieldName: String = "_raw",
   var output: Seq[NamedExpression] = Seq()) {

  def copy(indexName: String = this.indexName,
           timeFieldName: String = this.timeFieldName,
           rawFieldName: String = this.rawFieldName,
           output: Seq[NamedExpression] = this.output) =
    new LogicalContext(indexName, timeFieldName, rawFieldName, output)
}

object Transpiler {
  private def parsePipeline(search: String): spl.Pipeline =
    parse(search, spl.SplParser.pipeline(_)) match {
      case Parsed.Success(value, _) => value
      case f: Parsed.Failure =>
        throw new AssertionError(f.msg)
    }

  /** Converts SPL AST to Databricks Runtime internal Logical Execution Plan */
  private def logicalPlan(search: String): LogicalPlan = {
    SplToCatalyst.pipeline(new LogicalContext(), parsePipeline(search))
  }

  /** Executes SPL query on Databricks Runtime */
  def toDataFrame(spark: SparkSession, search: String): DataFrame =
    Dataset.ofRows(spark, logicalPlan(search))

  /** Generates PySpark code out of SPL search */
  def toPython(search: String): String = {
    "(" + PythonGenerator.fromPlan(GeneratorContext(), logicalPlan(search)) + ")\n"
  }
}
