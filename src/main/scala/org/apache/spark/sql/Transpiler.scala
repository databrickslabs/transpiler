package org.apache.spark.sql

import fastparse.{Parsed, parse}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

private class LogicalContext(
   val indexName: String = "main",
   val timeFieldName: String = "_time",
   val rawFieldName: String = "_raw",
   val splFieldToAttr: (spl.Field) => NamedExpression = (field: spl.Field) => UnresolvedAttribute(Seq(field.value)),
   val analyzePlan: (LogicalPlan) => Seq[Attribute] = (plan: LogicalPlan) => Seq[Attribute](),
   var output: Seq[NamedExpression] = Seq()) {

  def copy(indexName: String = this.indexName,
           timeFieldName: String = this.timeFieldName,
           rawFieldName: String = this.rawFieldName,
           splFieldToAttr: (spl.Field) => NamedExpression = this.splFieldToAttr,
           analyzePlan: (LogicalPlan) => Seq[Attribute] = this.analyzePlan,
           output: Seq[NamedExpression] = this.output) =
    new LogicalContext(indexName, timeFieldName, rawFieldName,splFieldToAttr, analyzePlan, output)
}

object Transpiler {
  private def parsePipeline(search: String): spl.Pipeline =
    parse(search, spl.SplParser.pipeline(_)) match {
      case Parsed.Success(value, _) => value
      case f: Parsed.Failure =>
        throw new AssertionError(f.msg)
    }

  /** Converts SPL AST to Databricks Runtime internal Logical Execution Plan */
  private def logicalPlan(search: String, logicalContext: LogicalContext = new LogicalContext()): LogicalPlan = {
    SplToCatalyst.pipeline(logicalContext, parsePipeline(search))
  }

  /** Executes SPL query on Databricks Runtime */
  def toDataFrame(spark: SparkSession, search: String): DataFrame = {
    Dataset.ofRows(spark, logicalPlan(search, new LogicalContext(
      // TODO: make indexName, timeFieldName, rawFieldName configurable through Spark config
      // TODO: introduce callback for column renames, probably with a simple rule framework
      analyzePlan = (table: LogicalPlan) => {
        val queryExecution = spark.sessionState.executePlan(table)
        queryExecution.assertAnalyzed()
        queryExecution.analyzed.output
      }
    )))
  }
  /** Generates PySpark code out of SPL search */
  def toPython(search: String): String = {
    "(" + PythonGenerator.fromPlan(GeneratorContext(), logicalPlan(search)) + ")\n"
  }
}
