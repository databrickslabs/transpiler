package org.apache.spark.sql

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Filter, Limit, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.{expressions => e}
import spl.IntValue

class SplToCatalyst {
  /**
   * Currently projected expressions
   * TODO: https://github.com/databricks/spark-spl/issues/2
   */
  private val LOGGER: Logger = Logger.getLogger(this.getClass)
  private var output = Seq[e.NamedExpression]()

  def process(p: spl.Pipeline): LogicalPlan = p.commands.foldLeft(
      UnresolvedRelation(Seq("x")).asInstanceOf[LogicalPlan]) { (tree, command) =>
      command match {
        case spl.EvalCommand(fields) =>
          fields.foldLeft(tree) { (plan, field) =>
            val (spl.Value(name), expr) = field
            withColumn(plan, name, expr)
          }

        case spl.WhereCommand(expr) =>
          Filter(expression(expr), tree)

        case spl.SearchCommand(expr) =>
          // probably search is different from where...
          Filter(expression(expr), tree)

        case spl.TableCommand(fields) =>
          Project(fields.map {
            case spl.Value(value) => Column(value)
          }.map(_.named), tree)

        case spl.ConvertCommand(timeformat, convs) =>
          convs.foldLeft(tree) { (plan, fc) =>
            val name = fc.alias.getOrElse(fc.field).value
            withColumn(plan, name, spl.Call(fc.func, Seq(fc.field)))
          }

        case spl.HeadCommand(evalExpr, keepLast, nullOption) =>
          // TODO Implement keeplast and null options behaviour
          LOGGER.debug(s"Adding `HeadCommand` with options: ${evalExpr} to the tree")
          if (evalExpr.isInstanceOf[IntValue])
            Limit(expression(evalExpr), tree)
          else
            Filter(expression(evalExpr), tree)

        case spl.LookupCommand(options, dataset, fields, output) =>
          // TODO: implement it as joins later
          tree

        case spl.CollectCommand(args, fields) =>
          //fields.map(fieldName => Column(fieldName.value))
          // TODO: add projection if fields is not empty
          AppendData(UnresolvedRelation(Seq(args("index"))), tree, Map(), isByName = true)
      }
    }

  private def withColumn(tree: LogicalPlan, name: String, expr: spl.Expr): Project = {
    output = output :+ e.Alias(expression(expr), name)()
    Project(output, tree)
  }

  private def mapCall(call: spl.Call): e.Expression = call.name match {
    case "isnull" =>
      e.IsNull(expression(call.args.head))
    case "ctime" =>
      val field = attr(call.args.head)
      Column(field).cast("date").as(field.name).named
  }

  private def expression(expr: spl.Expr): e.Expression = expr match {
    case constant: spl.Constant => mapConstants(constant)
    case call: spl.Call => mapCall(call)
    case spl.Unary(symbol, right) => symbol match {
      case spl.UnaryNot => e.Not(expression(right))
      // TODO: failure modes
    }
    case spl.Binary(left, symbol, right) => symbol match {
      case straight: spl.Straight => straight match {
        case relational: spl.Relational => relational match {
          case spl.LessThan => e.LessThan(attr(left), expression(right))
          case spl.GreaterThan =>e.GreaterThan(attr(left), expression(right))
          case spl.GreaterEquals => e.GreaterThanOrEqual(attr(left), expression(right))
          case spl.LessEquals => e.LessThanOrEqual(attr(left), expression(right))
          case spl.Equals => e.EqualTo(attr(left), expression(right))
          case spl.NotEquals => e.Not(e.EqualTo(attr(left), expression(right)))
        }
        case spl.Or => e.Or(expression(left), expression(right))
        case spl.And => e.And(expression(left), expression(right))
        // TODO: make a failure case
      }
      // TODO: make a failure case
    }
  }

  private def attr(expr: spl.Expr): UnresolvedAttribute = expr match {
    case spl.Value(value) => UnresolvedAttribute(Seq(value))
    // TODO: failure mode
  }

  private def mapConstants(constant: spl.Constant): e.Literal = constant match {
    case spl.Null() => e.Literal.create(null)
    case spl.Bool(value) => e.Literal.create(value)
    case spl.Value(value) => e.Literal.create(value)
    case spl.IntValue(value) => e.Literal.create(value)
  }
}
