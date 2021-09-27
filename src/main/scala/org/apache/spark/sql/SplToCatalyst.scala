package org.apache.spark.sql

import scala.collection.mutable
import scala.util.matching.Regex
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedRegex, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Max, Min}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Cast, Descending, Expression, Literal, NamedExpression, Not, RLike, RegExpExtract, Round, SortOrder}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AppendData, Deduplicate, Filter, Join, JoinHint, Limit, LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.{expressions => e}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}
import spl.{Expr, Value}

import scala.collection.mutable.ListBuffer

class SplToCatalyst extends Logging {
  /**
   * Currently projected expressions
   * TODO: https://github.com/databricks/spark-spl/issues/2
   */
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
          selectExpr(fields, tree)

        case spl.ConvertCommand(timeformat, convs) =>
          convs.foldLeft(tree) { (plan, fc) =>
            val name = fc.alias.getOrElse(fc.field).value
            withColumn(plan, name, spl.Call(fc.func, Seq(fc.field)))
          }

        case spl.HeadCommand(expr, keepLast, nullOption) =>
          // TODO Implement keeplast and null options behaviour
          logger.debug(s"Adding `HeadCommand` with options: $expr to the tree")
          if (expr.isInstanceOf[spl.IntValue])
            Limit(expression(expr), tree)
          else
            Filter(expression(expr), tree)

        case spl.SortCommand(fields) =>
          val sortOrderSeq: Seq[SortOrder] = sortOrder(fields)
          Sort(sortOrderSeq, global = true, tree)

        case spl.FieldsCommand(op, fields) =>
          if (op.getOrElse("+").equals("-")) {
            val fieldsToDiscard = fields.map(_.value.replace("*", "(.*)")).mkString("|")
            val columnRegex = UnresolvedRegex(s"(?!$fieldsToDiscard).*", None, caseSensitive = false)
            Project(Seq(columnRegex), tree)
          } else
            selectExpr(fields, tree)

        case spl.LookupCommand(dataset, fields, output) =>
          leftJoinUsing(dataset, fields, output, tree)

        case spl.CollectCommand(args, fields) =>
          // fields.map(fieldName => Column(fieldName.value))
          // TODO: add projection if fields is not empty
          AppendData(UnresolvedRelation(Seq(args("index"))), tree, Map(), isByName = true)

        case spl.StatsCommand(params, funcs, by, dedupSplitVals) =>
          if (dedupSplitVals) {
            Deduplicate(by.map(x => UnresolvedAttribute(x.value)),
              aggregate(by, funcs, tree))
          } else {
            aggregate(by, funcs, tree)
          }

        case spl.RexCommand(field, maxMatch, offsetField, mode, regex) =>
          // TODO find a way to implement max_match, offset_field and mode
          rexExtract(field, maxMatch, offsetField, mode, regex, tree)

        case spl.RenameCommand(aliases) =>
          renameColumn(aliases, tree)

        case spl.RegexCommand(item, regex) =>
          item match {
            case Some(value) =>
              val catalystOp = if (value._2.contains("!")) Not else (expr: Expression) => expr
              Filter(catalystOp(RLike(Column(value._1.value).expr, Literal(regex))), tree)
            case None =>
              Filter(RLike(Column("_raw").expr, Literal(regex)), tree)
          }

        case spl.JoinCommand(joinType, useTime, earlier,
                             overwrite, max, fields, subSearch) =>
          Join(tree, process(subSearch),
               UsingJoin(joinType match {
                 case "inner" => Inner
                 case "left" => LeftOuter
                 case "outer" => LeftOuter
                 case _ => Inner
               }, fields.map(_.value)), None, JoinHint.NONE)

        case spl.ReturnCommand(count, fields) =>
          val countLimit = count match {
            case Some(item) => Literal(item.value)
            case None => Literal(1)
          }

          Limit(countLimit, Project(fields.map {
            case alias: (Value, Expr) =>
              e.Alias(Column(attr(alias._2).name).named, attr(alias._1).name)()
            case field: Value =>
              Column(field.value).named
          }, tree))

        case spl.FillNullCommand(value, fields) =>
          val fieldsOpt = fields.getOrElse(Seq.empty[Value]).map(_.value).toSet
          FillNullShim(value.getOrElse("0"), fieldsOpt, tree)
      }
    }

  private def leftJoinUsing(dataset: String,
                            fields: Seq[spl.Field],
                            output: Option[spl.LookupOutput],
                            tree: LogicalPlan): Join = {
    val hasAliases: Boolean = fields exists {
      case _: spl.Value => false
      case _: spl.Alias => true
      case _: spl.AliasedField => true
    }
    var right = UnresolvedRelation(Seq(dataset)).asInstanceOf[LogicalPlan]
    if (hasAliases || output.isDefined) {
      right = Project(fields.map(fieldOrAlias) ++ (output match {
        case Some(spl.LookupOutput(kv, outputFields)) =>
          outputFields.map(fieldOrAlias)
        case None => Seq()
      }), right)
    }
    Join(tree, right,
      UsingJoin(LeftOuter, fields.map {
        case spl.Value(fieldName) => fieldName
        case spl.AliasedField(_, alias) => alias
      }), None, JoinHint.NONE)
  }

  private def fieldOrAlias(field: spl.Field): NamedExpression = field match {
    case spl.Value(fieldName) =>
      UnresolvedAttribute(fieldName)
    case spl.AliasedField(spl.Value(fieldName), alias) =>
      e.Alias(UnresolvedAttribute(fieldName), alias)()
    case spl.Alias(expr, alias) =>
      e.Alias(expression(expr), alias)()
  }

  private def aggregate(by: Seq[spl.Value], funcs: Seq[spl.Expr], tree: LogicalPlan) =
    Aggregate(fieldList(by), aggregates(funcs), tree)

  private def fieldList(fields: Seq[spl.Value]): Seq[Expression] = fields.map {
    case spl.Value(value) => Column(value)
  }.map(_.named)

  private def aggregates(funcs: Seq[spl.Expr]): Seq[NamedExpression] = funcs.map {
    case call: spl.Call =>
      e.Alias(function(call), call.name)()
    case spl.Alias(call: spl.Call, name) =>
      e.Alias(function(call), name)()
    // TODO: add failure case
  }

  private def withColumn(tree: LogicalPlan, name: String, expr: spl.Expr): Project = {
    output = output :+ e.Alias(expression(expr), name)()
    Project(output, tree)
  }

  private def function(call: spl.Call): e.Expression = call.name match {
    case "isnull" =>
      e.IsNull(attrOrExpr(call.args.head))
    case "ctime" =>
      val field = attr(call.args.head)
      Column(field).cast("date").as(field.name).named
    case "count" =>
      Count(call.args.map(expression))
    case "min" =>
      // TODO: would currently fail on wildcard attributes
      Min(attrOrExpr(call.args.head))
    case "max" =>
      // TODO: would currently fail on wildcard attributes
      Max(attr(call.args.head))
    case "round" =>
      val num = attrOrExpr(call.args.head)
      val scale = call.args.lift(1).map(expression).getOrElse(Literal(0))
      Round(num, scale)
    case "TERM" =>
      Term(expression(call.args.head))
    case _ =>
      val approx = s"${call.name}(${call.args.map(_.toString).mkString(",")})"
      throw new AnalysisException(s"Unknown SPL function: $approx")
  }

  private def attrOrExpr(expr: spl.Expr) = expr match {
    case spl.Value(value) => UnresolvedAttribute(Seq(value))
    case _ => expression(expr)
  }

  private def expression(expr: spl.Expr): e.Expression = expr match {
    case constant: spl.Constant => mapConstants(constant)
    case call: spl.Call => function(call)
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
        case spl.Add => e.Add(expression(left), expression(right))
        case spl.Subtract => e.Subtract(expression(left), expression(right))
        case spl.Multiply => e.Multiply(expression(left), expression(right))
        case spl.Divide => e.Divide(expression(left), expression(right))
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

  private def rexParseNamedGroup(inputString: String): mutable.Map[String, Int] = {
    val namedGroupPattern: Regex = "<([a-zA-Z_0-9]+)>".r
    val namedGroupMap = mutable.Map[String, Int]()
    var index = 1
    for (patternMatch <- namedGroupPattern.findAllMatchIn(inputString)) {
      namedGroupMap(patternMatch.group(1)) = index
      index += 1
    }
    namedGroupMap
  }

  private def selectExpr(fields: Seq[spl.Value], tree: LogicalPlan) = {
    Project(fields.map {
      case spl.Value(value) => Column(value)
    }.map(_.named), tree)
  }

  private def sortOrder(fields: Seq[(Option[String], spl.Expr)]): Seq[SortOrder] = {
    fields map {
      case Tuple2(a, b) => (b, if (a.getOrElse("+") == "-") Descending else Ascending)
    } map {
      case (spl.Call(name, args), order) => name match {
        case "num" =>
          SortOrder(Cast(attr(args.head), DoubleType), order)
        case "str" =>
          SortOrder(Cast(attr(args.head), StringType), order)
        case "ip" =>
          // TODO implement logic for ip function
          // see https://docs.splunk.com/Documentation/Splunk/latest/SearchReference/sort
          SortOrder(attr(args.head), order)
        case _ =>
          SortOrder(attr(args.head), order)
      }
      case (spl.Value(value), order) =>
        SortOrder(UnresolvedAttribute(value), order)
    }
  }

  private def rexExtract(field: Option[spl.Value],
                         maxMatch: Option[spl.IntValue],
                         offsetField: Option[spl.Value],
                         mode: Option[spl.Value],
                         regex: String,
                         tree: LogicalPlan): Project = {
    // TODO _raw column configurable
    mode match {
      case Some(value) => throw new NotImplementedError(s"rex mode=${value.value} [...] currently not supported !")
      case None =>
        val myList = new ListBuffer[NamedExpression]()
        myList += UnresolvedRegex("^.*?", None, caseSensitive = false)
        rexParseNamedGroup(regex) foreach {
          case (colName, groupIndex)  =>
            val alias = e.Alias(
              RegExpExtract(
                field match {
                  case Some(value) => Column(value.value).expr
                  case None => Column("_raw").expr
                },
                Literal(regex),
                Literal(groupIndex)), colName)()
            myList += alias
        }
        Project(myList, tree)
    }
  }

  private def renameColumn(aliases: Seq[spl.Alias], tree: LogicalPlan): LogicalPlan = {
    val myList = new ListBuffer[NamedExpression]
    val regex = aliases.map(alias => attr(alias.expr).name).mkString("|")
    myList += UnresolvedRegex(s"(?!$regex).*", None, caseSensitive = false)
    aliases foreach {alias => {
      myList += e.Alias(attr(alias.expr), alias.name)()
    }}
    Project(myList, tree)
  }
}