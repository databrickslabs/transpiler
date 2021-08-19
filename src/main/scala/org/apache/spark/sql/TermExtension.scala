package org.apache.spark.sql

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, Expression, ExpressionDescription, ExpressionInfo, Or, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{BooleanType, DataType, StringType}

class TermExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(new TermExpansion(_))

    val clazz = classOf[Term]
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    extensions.injectFunction((FunctionIdentifier("term"), new ExpressionInfo(
      clazz.getCanonicalName,null,"term", df.usage(),
      df.arguments(), df.examples(), df.note(), df.group(),
      df.since(), df.deprecated()),
      exprs => Term(exprs.head)))
  }
}

class TermExpansion(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case Term(child) =>
      expand(plan.output
        .filter(_.dataType == StringType),
        child)
  }

  private def expand(attrs: Seq[Attribute], expr: Expression): Expression =
    attrs match {
      case last :: Nil => EqualTo(last, expr)
      case head :: tail => Or(EqualTo(head, expr), expand(tail, expr))
    }
}

/**
 * Placeholder function for term expansion
 */
@ExpressionDescription(
  usage = "_FUNC_(str) - Searches for str in all columns that are accessible",
  examples = """
    Examples:
      > SELECT 'a' AS a, 'b' AS b, 'c' AS c, _FUNC_('a') AS found;
       a, b, c, true
      > SELECT 'a' AS a, 'b' AS b, 'c' AS c, _FUNC_('d') AS found;
       a, b, c, false
  """,
  since = "3.3.0")
case class Term(child: Expression) extends Unevaluable {
  override def nullable: Boolean = false
  override def dataType: DataType = BooleanType
  override def children: Seq[Expression] = Seq(child)
}