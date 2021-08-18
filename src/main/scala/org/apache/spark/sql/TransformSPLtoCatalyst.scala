package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, IsNull, Literal, NamedExpression, Not}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.{expressions => e}
import splunkql._

class TransformSPLtoCatalyst {
  /** Currently projected expressions */
  private var output = Seq[NamedExpression]()

  def process(p: Pipeline): LogicalPlan = p.commands.foldLeft(
        UnresolvedRelation(Seq("x")).asInstanceOf[LogicalPlan]) { (tree, command) =>
      command match {
        case EvalCommand(fields) => fields.foldLeft(tree) { (plan, field) =>
          val (Value(name), expr) = field
          withColumn(plan, name, expr)
        }

        case WhereCommand(expr) => Filter(expression(expr), tree)

        // probably search is different from where...
        case SearchCommand(expr) => Filter(expression(expr), tree)

        case TableCommand(fields) => Project(fields.map {
          case Value(value) => Column(value)
        }.map(_.named), tree)

        case ConvertCommand(timeformat, convs) => convs.foldLeft(tree) { (plan, fc) =>
          val name = fc.alias.getOrElse(fc.field).value
          withColumn(plan, name, Call(fc.func, Seq(fc.field)))
        }

        case LookupCommand(options, dataset, fields, output) =>
          // TODO: implement it as joins later
          tree

        case CollectCommand(args, fields) =>
          //fields.map(fieldName => Column(fieldName.value))
          // TODO: add projection if fields is not empty
          AppendData(UnresolvedRelation(Seq(args("index"))), tree, Map(), isByName = true)
      }
    }

  def withColumn(tree: LogicalPlan, name: String, expr: Expr): Project = {
    output = output :+ Alias(expression(expr), name)()
    Project(output, tree)
  }

  def mapCall(call: Call): Expression = call.name match {
    case "isnull" => IsNull(expression(call.args.head))
    case "ctime" =>
      val field = attr(call.args.head)
      Column(field).cast("date").as(field.name).named
  }

  def expression(expr: Expr): Expression = expr match {
    case constant: Constant => mapConstants(constant)
    case call: Call => mapCall(call)
    case Unary(symbol, right) => symbol match {
      case UnaryNot => Not(expression(right))
      // TODO: failure modes
    }
    case Binary(left, symbol, right) => symbol match {
      case straight: Straight => straight match {
        case relational: Relational => relational match {
          case LessThan => e.LessThan(attr(left), expression(right))
          case GreaterThan =>e.GreaterThan(attr(left), expression(right))
          case GreaterEquals => e.GreaterThanOrEqual(attr(left), expression(right))
          case LessEquals => e.LessThanOrEqual(attr(left), expression(right))
          case Equals => e.EqualTo(attr(left), expression(right))
          case NotEquals => Not(e.EqualTo(attr(left), expression(right))) // TODO: ask if this is fine
        }
        case Or => e.Or(expression(left), expression(right))
        case And => e.And(expression(left), expression(right))
        // TODO: make a failure case
      }
      // TODO: make a failure case
    }
  }

  def attr(expr: Expr): UnresolvedAttribute = expr match {
    case Value(value) => UnresolvedAttribute(Seq(value))
    // TODO: failure mode
  }

  def mapConstants(constant: Constant): Literal = constant match {
    case Null() => Literal.create(null)
    case Bool(value) => Literal.create(value)
    case Value(value) => Literal.create(value)
    case IntValue(value) => Literal.create(value)
  }
}
