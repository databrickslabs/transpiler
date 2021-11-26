package org.apache.spark.sql

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class SplExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(spark => new TermExpansion(spark))
    extensions.injectResolutionRule(spark => new FillNullShimExpansion(spark))
    registerTerm(extensions)
  }

  private def registerTerm(extensions: SparkSessionExtensions): Unit = {
    val clazz = classOf[Term]
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    extensions.injectFunction((FunctionIdentifier("term"), new ExpressionInfo(
      clazz.getCanonicalName, null, "term", df.usage(),
      df.arguments(), df.examples(), df.note(), df.group(),
      df.since(), df.deprecated()), {
      case Seq(expr) => Term(expr)
      case _ => throw new AnalysisException("TERM() expects only single argument")
    }))
  }
}

object WrappedDataset {
  /** Make Dataset.ofRows accessible outside spark sql package */
  def ofRows(session: SparkSession, plan: LogicalPlan): DataFrame = Dataset.ofRows(session, plan)
}
