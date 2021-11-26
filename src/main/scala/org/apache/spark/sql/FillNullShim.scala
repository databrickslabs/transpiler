package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OrderPreservingUnaryNode, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions.{coalesce, lit, nanvl}
import org.apache.spark.sql.types.{DoubleType, FloatType}

// This logical plan is only there to shim Splunk's fillnull command logic, when
// SPL is executed on Spark. Generated Python code still refers to .na.fill()
// Given rule replicates the logic of .na.fill()
class FillNullShimExpansion(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case FillNullShim(value, columns, child) =>
      Project(child.schema map { item =>
        val column = Column(item.name)
        if (columns.isEmpty | columns.contains(item.name)) {
          coalesce(item.dataType match {
            case DoubleType | FloatType =>
              nanvl(column, lit(null))
            case _ => column
          }, lit(value).cast(item.dataType)).alias(item.name).named
        } else {
          column.named
        }
      }, child)
  }
}

case class FillNullShim(value: String, columns: Set[String], child: LogicalPlan)
  extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = throw new UnresolvedException(this, "FillNullShim")
  override lazy val resolved: Boolean = false
}
