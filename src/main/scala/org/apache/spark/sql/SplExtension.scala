package org.apache.spark.sql

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OrderPreservingUnaryNode, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions.{coalesce, lit, nanvl}
import org.apache.spark.sql.types._

class SplExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(spark => new TermExpansion(spark))
    extensions.injectResolutionRule(spark => new FillNullShimExpansion(spark))

    val clazz = classOf[Term]
    val df = clazz.getAnnotation(classOf[ExpressionDescription])

    extensions.injectFunction((FunctionIdentifier("term"), new ExpressionInfo(
      clazz.getCanonicalName,null,"term", df.usage(),
      df.arguments(), df.examples(), df.note(), df.group(),
      df.since(), df.deprecated()), {
        case Seq(expr) => Term(expr)
        case _ => throw new AnalysisException("TERM() expects only single argument")
      }))

  }
}

class TermExpansion(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformExpressions {
      case Term(child) =>
        if (!child.resolved) {
          throw new AnalysisException(s"Child expression must be resolved: $child")
        }
        expand(plan.output.filter(_.dataType == StringType), child)
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

/**
 * This logical plan is only there to shim Splunk's fillnull command logic, when
 * SPL is executed on Spark. Generated Python code still refers to .na.fill()
 *
 * Given rule replicates the logic of .na.fill()
 */
class FillNullShimExpansion(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform  {
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
