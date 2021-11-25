package spl.catalyst

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import spl.ast.Field

private[spl] class LogicalContext(
     val indexName: String = "main",
     val timeFieldName: String = "_time",
     val rawFieldName: String = "_raw",
     val splFieldToAttr: Field => NamedExpression = field => UnresolvedAttribute(Seq(field.value)),
     val analyzePlan: LogicalPlan => Seq[Attribute] = (_: LogicalPlan) => Seq[Attribute](),
     var output: Seq[NamedExpression] = Seq()) {
  def copy(indexName: String = this.indexName,
           timeFieldName: String = this.timeFieldName,
           rawFieldName: String = this.rawFieldName,
           splFieldToAttr: (Field) => NamedExpression = this.splFieldToAttr,
           analyzePlan: (LogicalPlan) => Seq[Attribute] = this.analyzePlan,
           output: Seq[NamedExpression] = this.output): LogicalContext =
    new LogicalContext(indexName, timeFieldName, rawFieldName, splFieldToAttr, analyzePlan, output)
}
