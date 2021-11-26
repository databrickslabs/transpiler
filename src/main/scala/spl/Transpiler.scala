package spl

import fastparse.{Parsed, parse}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, SparkSession, WrappedDataset}
import spl.catalyst._
import spl.pyspark.{GeneratorContext, PythonGenerator}


object Transpiler {
  import spl.ast.Pipeline
  import spl.parser.SplParser

  private def parsePipeline(search: String): Pipeline =
    parse(search, SplParser.pipeline(_)) match {
      case Parsed.Success(value, _) => value
      case f: Parsed.Failure =>
        // scalastyle:off
        throw new AssertionError(f.msg)
        // scalastyle:on
    }

  /** Converts SPL AST to Databricks Runtime internal Logical Execution Plan */
  private def logicalPlan(search: String,
                          logicalContext: LogicalContext = new LogicalContext()): LogicalPlan =
    SplToCatalyst.pipeline(logicalContext, parsePipeline(search))

  /** Executes SPL query on Databricks Runtime */
  def toDataFrame(spark: SparkSession, search: String): DataFrame =
    WrappedDataset.ofRows(spark, logicalPlan(search, configuredLogicalContext(spark)))

  private def configuredLogicalContext(spark: SparkSession) = new LogicalContext(
    // TODO: introduce callback for column renames, probably with a simple rule framework
    analyzePlan = (table: LogicalPlan) => {
      val queryExecution = spark.sessionState.executePlan(table)
      queryExecution.assertAnalyzed()
      queryExecution.analyzed.output
    },
    indexName = spark.conf.get("spl.index", "main"),
    timeFieldName = spark.conf.get("spl.field._time", "_time"),
    rawFieldName = spark.conf.get("spl.field._raw", "_raw"))

  /** Generates PySpark code out of SPL search */
  def toPython(search: String): String = {
    "(" + PythonGenerator.fromPlan(GeneratorContext(), logicalPlan(search)) + ")\n"
  }

  /** Generates PySpark code out of SPL search */
  def toPython(spark: SparkSession, search: String): String = {
    "(" + PythonGenerator.fromPlan(GeneratorContext(
      maxLineWidth = spark.conf.get("spl.generator.lineWidth", "120").toInt
    ), logicalPlan(search, configuredLogicalContext(spark))) + ")\n"
  }
}
