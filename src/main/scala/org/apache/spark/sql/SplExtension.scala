package org.apache.spark.sql

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class SplExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(spark => new TermExpansion(spark))
    extensions.injectResolutionRule(spark => new FillNullShimExpansion(spark))
    registerTerm(extensions)
    registerCidrMatch(extensions)
  }

  private def registerTerm(extensions: SparkSessionExtensions): Unit = {
    val clazz = classOf[Term]
    extensions.injectFunction((FunctionIdentifier("term"),
      new ExpressionInfo(clazz.getCanonicalName, "term"), {
        case Seq(expr) => Term(expr)
        case _ => throw new AnalysisException("TERM() expects only single argument")
    }))
  }

  private def registerCidrMatch(extensions: SparkSessionExtensions): Unit = {
    val clazz = classOf[CidrMatch]
    extensions.injectFunction((FunctionIdentifier("cidr_match"),
      new ExpressionInfo(clazz.getCanonicalName, "cidr_match"), {
        case Seq(cidr, ip) => CidrMatch(cidr, ip)
        case _ => throw new AnalysisException("CIDR_MATCH() expects two arguments")
    }))
  }
}

object WrappedDataset {
  /** Make Dataset.ofRows accessible outside spark sql package */
  def ofRows(session: SparkSession, plan: LogicalPlan): DataFrame = Dataset.ofRows(session, plan)
}
