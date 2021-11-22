package spl

import fastparse.{Parsed, parse}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, SparkSession, WrappedDataset}
import spl.ast.Pipeline
import spl.catalyst._
import spl.parser.SplParser
import spl.pyspark.{GeneratorContext, PythonGenerator}


object Transpiler {
  private def parsePipeline(search: String): Pipeline =
    parse(search, SplParser.pipeline(_)) match {
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
    WrappedDataset.ofRows(spark, logicalPlan(search, new LogicalContext(
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
