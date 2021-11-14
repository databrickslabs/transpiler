package org.apache.spark.sql

import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.control.NonFatal
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, UsingJoin}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.unsafe.types.UTF8String

object SplToCatalyst extends Logging {
  def pipeline(ctx: LogicalContext, p: spl.Pipeline): LogicalPlan = {
    val (table, pipe) = determineTable(ctx, p)
    pipe.commands.foldLeft(table) {
      wrapCommand(ctx) {
        (tree, command) => command match {
          case spl.SearchCommand(expr) =>
            // probably search is different from where...
            Filter(expression(ctx, expr), tree)

          case inputLookup: spl.InputLookup =>
            // TODO implement `append`, `strict` and `start` options, also inputlookup should accept file name
            applyInputLookup(ctx, tree, inputLookup)

          case spl.WhereCommand(expr) =>
            Filter(expression(ctx, expr), tree)

          case spl.EvalCommand(fields) =>
            fields.foldLeft(tree) { (plan, field) =>
              val (spl.Field(name), expr) = field
              withColumn(ctx, plan, name, expr)
            }

          case spl.TableCommand(fields) =>
            selectExpr(fields, tree)

          case spl.ConvertCommand(timeformat, convs) =>
            convs.foldLeft(tree) { (plan, fc) =>
              val name = fc.alias.getOrElse(fc.field).value
              withColumn(ctx, plan, name, spl.Call(fc.func, Seq(fc.field)))
            }

          case spl.HeadCommand(expr, keepLast, nullOption) =>
            // TODO Implement keeplast and null options behaviour
            log.debug(s"Adding `HeadCommand` with options: $expr to the tree")
            if (expr.isInstanceOf[spl.IntValue]) Limit(expression(ctx, expr), tree)
            else Filter(expression(ctx, expr), tree)

          case spl.SortCommand(fields) =>
            Sort(sortOrder(fields), global = true, tree)

          case spl.FieldsCommand(rmFields, fields) =>
            applyFields(ctx, tree, rmFields, fields)

          case spl.LookupCommand(dataset, fields, output) =>
            leftJoinUsing(ctx, dataset, fields, output, tree)

          case spl.CollectCommand(args, fields) =>
            // fields.map(fieldName => Column(fieldName.value))
            // TODO: add projection if fields is not empty
            AppendData(UnresolvedRelation(Seq(args("index"))), tree, Map(), isByName = true)

          case spl.StatsCommand(params, funcs, by, dedupSplitVals) =>
            val agg = aggregate(ctx, by, funcs, tree)
            if (dedupSplitVals) {
              Deduplicate(by.map(x => UnresolvedAttribute(x.value)), agg)
            } else agg

          case rc: spl.RexCommand =>
            // TODO find a way to implement max_match, offset_field and mode
            applyRex(ctx, tree, rc)

          case spl.RenameCommand(aliases) =>
            applyRename(ctx, tree, aliases)

          case spl.RegexCommand(item, regex) =>
            applyRegex(ctx, tree, item, regex)

          case jc: spl.JoinCommand =>
            applyJoin(ctx, tree, jc)

          case spl.ReturnCommand(count, fieldsOrAliases) =>
            applyReturn(ctx, tree, count, fieldsOrAliases)

          case spl.FillNullCommand(value, fields) =>
            val fieldsOpt = fields.getOrElse(Seq.empty[spl.Field]).map(_.value).toSet
            FillNullShim(value.getOrElse("0"), fieldsOpt, tree)

          case spl.EventStatsCommand(params, funcs, by) =>
            // TODO implement allnum option
            applyEventStats(ctx, tree, params, funcs, by)

          case spl.StreamStatsCommand(funcs, by, current, window) =>
            applyStreamStats(ctx, tree, funcs, by, current, window)

          case dedupCmd: spl.DedupCommand =>
            applyDedup(ctx, tree, dedupCmd)

          case fc: spl.FormatCommand =>
            // TODO Implement behaviour with mvsep when multiple value field is present
            applyFormat(ctx, tree, fc)

          case spl.MvCombineCommand(delim, field) =>
            applyMvCombine(ctx, tree, field, delim)

          case spl.MvExpandCommand(field, limit) =>
            applyMvExpand(ctx, tree, field, limit)

          case bc: spl.BinCommand =>
            applyBin(ctx, tree, bc)
        }
      }
    }
  }

  private def function(ctx: LogicalContext, call: spl.Call): Expression = call.name match {
    case "isnull" =>
      IsNull(attrOrExpr(ctx, call.args.head))
    case "if" =>
      val branches = Seq((attrOrExpr(ctx, call.args.head), attrOrExpr(ctx, call.args(1))))
      val elseValue = Some(attrOrExpr(ctx, call.args(2)))
      CaseWhen(branches, elseValue)
    case "ctime" =>
      val field = attr(call.args.head)
      Column(field).cast("date").as(field.name).named
    case "count" =>
      AggregateExpression(
        Count(call.args match {
          case Nil => Seq(Literal.create(1))
          case args => args.map(expression(ctx, _))
        }), Complete, isDistinct = false)
    case "sum" =>
      Sum(attr(call.args.head))
    case "tonumber" =>
      Cast(attr(call.args.head), DoubleType)
    case "min" =>
      // TODO: would currently fail on wildcard attributes
      determineMin(ctx, call)
    case "max" =>
      // TODO: would currently fail on wildcard attributes
      determineMax(ctx, call)
    case "len" =>
      Length(attrOrExpr(ctx, call.args.head))
    case "substr" =>
      val str = attrOrExpr(ctx, call.args.head)
      val pos = expression(ctx, call.args(1))
      val len = call.args.lift(2).map(expression(ctx, _)).getOrElse(Literal(Integer.MAX_VALUE))
      Substring(str,pos,len)
    case "coalesce" =>
      Coalesce(call.args.map(attrOrExpr(ctx, _)))
    case "round" =>
      val num = attrOrExpr(ctx, call.args.head)
      val scale = call.args.lift(1).map(expression(ctx, _)).getOrElse(Literal(0))
      Round(num, scale)
    case "TERM" =>
      Term(expression(ctx, call.args.head))
    case "values" =>
      CollectSet(attrOrExpr(ctx, call.args.head))
    case "earliest" =>
      First(attrOrExpr(ctx, call.args.head), ignoreNulls = true)
    case "latest" =>
      Last(attrOrExpr(ctx, call.args.head), ignoreNulls = true)
    case "strftime" =>
      determineStrftime(ctx, call)
    case "mvcount" =>
      Size(attrOrExpr(ctx, call.args.head))
    case "mvindex" =>
      val mvfield = attrOrExpr(ctx, call.args.head)
      val start = extractIndex(call.args(1))
      val stop = call.args.lift(2).map(extractIndex(_)).getOrElse(start)
      if (start * stop < 0) {
        throw new AnalysisException(s"A combination of negative and positive start and stop indices is not supported. start_index: ${start} stop_index: ${stop}")
      }
      val length : Int = stop - start + 1
      Slice(mvfield, Literal.create(start), Literal.create(length))
    case "mvappend" =>
      val inputFields : Seq[Expression] = call.args.map(attrOrExpr(ctx, _))
      Concat(inputFields)
    case "mvfilter" =>
      val expr = call.args.head
      val fields = extractFields(expr)
      if (fields.size > 1) {
        throw new AnalysisException(s"Expression references more than one field. Fields: ${fields}")
      }
      mvFilter(ctx, fields.head, expr)
    case "null" =>
      Literal(null)
    case "isnotnull" =>
      IsNotNull(attrOrExpr(ctx, call.args.head))
    case _ =>
      val approx = s"${call.name}(${call.args.map(_.toString).mkString(",")})"
      throw new AnalysisException(s"Unknown SPL function: $approx")
  }

  private def determineMin(ctx: LogicalContext, call: spl.Call): Expression = {
    call.args match {
       case Seq(spl.Field(v)) =>
         AggregateExpression(
           Min(attrOrExpr(ctx, call.args.head)),
           Complete, isDistinct = false)
       case Seq(_, _*) =>
         Least(call.args.map(attrOrExpr(ctx, _)))
    }
  }

  private def determineMax(ctx: LogicalContext, call: spl.Call): Expression = {
    call.args match {
      case Seq(spl.Field(v)) =>
        AggregateExpression(
          Max(attrOrExpr(ctx, call.args.head)),
          Complete, isDistinct = false)
      case Seq(_, _*) =>
        Greatest(call.args.map(attrOrExpr(ctx, _)))
    }
  }

  private def determineStrftime(ctx: LogicalContext, call: spl.Call): Expression = {
    DateFormatClass(attrOrExpr(ctx, call.args.head), Literal.create(call.args.lift(1) match {
      case Some(spl.Field(fmt)) => stftimeToDateFormat.foldLeft(fmt) {
        case (a, (b, c)) => a.replaceAll(b, c)
      }
      case Some(spl.StrValue(fmt)) => stftimeToDateFormat.foldLeft(fmt) {
        case (a, (b, c)) => a.replaceAll(b, c)
      }
      case _ => throw new AnalysisException(s"Invalid strftime format given")
    }))
  }

  private def mvFilter(ctx: LogicalContext, field: spl.Field, expr: spl.Expr) = {
    val filterArgument = attrOrExpr(ctx, field)
    val lambdaCtx = ctx.copy(splFieldToAttr = (field: spl.Field) => UnresolvedNamedLambdaVariable(Seq(field.value)))
    val filterFunction = LambdaFunction(expression(lambdaCtx, expr), Seq(UnresolvedNamedLambdaVariable(Seq(field.value))))
    ArrayFilter(filterArgument, filterFunction)
  }

  private def extractFields(expr: spl.Expr): Set[spl.Field] = expr match {
    case constant: spl.Constant => constant match {
      case field: spl.Field => Set(field)
      case _  => Set()
    }
    case spl.Unary(_, right) => extractFields(right)
    case spl.Binary(left, _, right) => extractFields(left)++extractFields(right)
    case spl.Call(_, args) => args.flatMap(extractFields).toSet
    case spl.FieldIn(_, exprs) => exprs.flatMap(extractFields).toSet
    case _ => Set()
  }

  private def extractIndex(x: spl.Expr, offset: Int = 1) : Int = x match {
    case spl.IntValue(index) => if (index >= 0) index + offset else index
    case _ => throw new AnalysisException("int expected")
  }

  private def isFilter(x: spl.Expr, name: String) = x match {
    case spl.Binary(spl.Field(field), spl.Equals, _) if field.equals(name) => true
    case _ => false
  }

  /** Finds indices in all of the binary nodes */
  private def findIndices(search: spl.Expr): Set[String] = search match {
    case b @ spl.Binary(_, _, spl.Field(value)) if isFilter(b, "index") => Set(value)
    case spl.Binary(left, _, right) => findIndices(left) ++ findIndices(right)
    case _ => Set()
  }

  /** Removes `index` filters, as they are lifted to the top of the tree */
  private def overwriteSplSearch(x: spl.Expr): spl.Expr = x match {
    case spl.Binary(left, spl.And, right) if isFilter(left, "index") => right
    case spl.Binary(left, spl.And, right) if isFilter(right, "index") => left
    // TODO: modify "earliest" filter
    case spl.Binary(left, symbol, right) => spl.Binary(overwriteSplSearch(left),
      symbol, overwriteSplSearch(right))
    case y: spl.Expr => y
  }

  /**
   * Index maps directly on Spark's table. Any filters on index are removed.
   * If no index is specified, the value is taken from the context
   */
  private def determineTable(ctx: LogicalContext, p: spl.Pipeline): (LogicalPlan, spl.Pipeline) = {
    val indices = p.commands.flatMap {
      case spl.SearchCommand(expr) => findIndices(expr)
      case inputLookup: spl.InputLookup => Set(inputLookup.tableName)
      case _ => Seq()
    }
    if (indices.size > 1) {
      throw new AnalysisException(s"Only one index allowed, but got: ${indices.mkString(",")}")
    }
    val tableName = indices.headOption.getOrElse(ctx.indexName)
    val table = UnresolvedRelation(Seq(tableName)).asInstanceOf[LogicalPlan]
    ctx.output ++= ctx.analyzePlan(table)
    // `filterNot` is being used here to filter out a `SearchCommand` that only contains `index`.
    // The idea is to remove the `index` filters, as they are lifted to the top of the tree
    (table, p.copy(commands = p.commands.filterNot {
        case s: spl.SearchCommand => isFilter(s.expr, "index")
        case _ => false
      }.map {
        case s: spl.SearchCommand => s.copy(expr = overwriteSplSearch(s.expr))
        case c: spl.Command => c
      }
    ))
  }

  private def leftJoinUsing(ctx: LogicalContext, dataset: String,
                            fields: Seq[spl.FieldLike],
                            output: Option[spl.LookupOutput],
                            tree: LogicalPlan): Join = {
    val hasAliases: Boolean = fields exists {
      case _: spl.Field => false
      case _: spl.Alias => true
      case _: spl.AliasedField => true
    }
    var right = UnresolvedRelation(Seq(dataset)).asInstanceOf[LogicalPlan]
    if (hasAliases || output.isDefined) {
      // TODO: modify LogicalContext output
      right = Project(fields.map(fieldOrAlias(ctx, _)) ++ (output match {
        case Some(spl.LookupOutput(kv, outputFields)) =>
          outputFields.map(fieldOrAlias(ctx, _))
        case None => Seq()
      }), right)
    }
    Join(tree, right,
      UsingJoin(LeftOuter, fields.map {
        case spl.Field(fieldName) => fieldName
        case spl.AliasedField(_, alias) => alias
      }), None, JoinHint.NONE)
  }

  private def fieldOrAlias(ctx: LogicalContext, field: spl.FieldLike): NamedExpression = field match {
    case spl.Field(fieldName) =>
      UnresolvedAttribute(fieldName)
    case spl.AliasedField(spl.Field(fieldName), alias) =>
      Alias(UnresolvedAttribute(fieldName), alias)()
    case spl.Alias(expr, alias) =>
      Alias(expression(ctx, expr), alias)()
  }

  private def aggregate(ctx: LogicalContext, by: Seq[spl.Field], funcs: Seq[spl.Expr], tree: LogicalPlan) = {
    // TODO: select _time
    val groupBy = by.map(attr)
    val agg = aggregates(ctx, funcs)
    val child = if (hasTimeFunctions(funcs)) Sort(Seq(
        SortOrder(UnresolvedAttribute(ctx.timeFieldName), Ascending, NullsFirst, Seq.empty)
    ), global = true, tree) else tree
    //TODO: hm... ctx.output = Seq()

    // new Aggregate(groupBy, agg, child) translates to
    // INVOKESPECIAL Aggregate.<init> (LSeq;Seq;LLogicalPlan;)V
    // and it's not filling in default arguments
    newAggregateIgnoringABI(groupBy, groupBy ++ agg, child)
  }

  // we're using Spark Catalyst's private APIs, so there are absolutely no guarantees
  // on binary compatibility of different Spark implementations. This is a hack to
  // make one of the most important SPL commands to be runnable in Databricks Runtime.
  private def newAggregateIgnoringABI(groupingExpressions: Seq[Expression],
              aggregateExpressions: Seq[NamedExpression],
              child: LogicalPlan): Aggregate = aggregateConstructorArgCountABI match {
    case 3 => Aggregate(groupingExpressions, aggregateExpressions, child)
    case 4 => aggregateConstructorReflection
      .newInstance(groupingExpressions, aggregateExpressions, child, None)
      .asInstanceOf[Aggregate]
    case _ => throw new AnalysisException(s"Incompatible runtime detected")
  }

  private val (aggregateConstructorReflection, aggregateConstructorArgCountABI) = {
    val firstConstructor = classOf[Aggregate].getConstructors.head
    (firstConstructor, firstConstructor.getParameterCount)
  }

  private def hasTimeFunctions(funcs: Seq[spl.Expr]) : Boolean = funcs.map {
    case spl.Call(name, _) => name
    case spl.Alias(spl.Call(name, _), _) => name
    case _ => ""
  } exists(Seq("earliest", "latest").contains(_))

  private def aggregates(ctx: LogicalContext, funcs: Seq[spl.Expr]): Seq[NamedExpression] = funcs.map {
    case call: spl.Call =>
      Alias(function(ctx, call), call.name)()
    case spl.Alias(call: spl.Call, name) =>
      Alias(function(ctx, call), name)()
    case x: spl.Expr =>
      throw new NotImplementedError(s"cannot convert aggregate: $x")
  }

  private def withColumn(ctx: LogicalContext,
                         tree: LogicalPlan,
                         name: String,
                         expr: spl.Expr): Project =
    withColumn(ctx, tree, name, attrOrExpr(ctx, expr))

  private def withColumn(ctx: LogicalContext,
                         tree: LogicalPlan,
                         name: String,
                         expression: Expression): Project =
    selectColumn(ctx, tree, Alias(expression, name)())

  private def selectColumn(ctx: LogicalContext,
                           tree: LogicalPlan,
                           ne: NamedExpression): Project = {
    ctx.output = ctx.output.map {
      case toReplace if toReplace.name == ne.name => ne
      case toKeep => toKeep
    }
    // To avoid duplicate column
    if (!ctx.output.map(_.name).contains(ne.name)) {
      // maybe there's a more functional way to do this
      ctx.output :+= ne
    }
    Project(ctx.output, tree)
  }

  private def selectColumns(ctx: LogicalContext,
                            tree: LogicalPlan,
                            namedExprs: Seq[NamedExpression]): Project = {
    ctx.output = namedExprs
    Project(ctx.output, tree)
  }

  private def wrapCommand(ctx: LogicalContext)
                         (mapper: (LogicalPlan, spl.Command) => LogicalPlan)
                         (plan: LogicalPlan, command: spl.Command): LogicalPlan =
    try {
      mapper(plan, command) match {
        case project: Project =>
          ctx.output = project.output
          project
        case agg: Aggregate =>
          // matching is done by attribute name because of ABI compatibility
          ctx.output = agg.groupingExpressions.filter(_.isInstanceOf[NamedExpression]).map {
            case ne: NamedExpression => UnresolvedAttribute(ne.name)
            } ++ agg.aggregateExpressions.map(ne => UnresolvedAttribute(ne.name))
          agg
        case result: LogicalPlan =>
          result
      }
    } catch {
      case NonFatal(e) =>
        val name = command.getClass.getSimpleName
          .replace("Command", "").toLowerCase
        log.warn(s"Error in $name", e)
        UnknownPlanShim(s"Error in $name: $e", plan)
    }

  private def selectExpr(fields: Seq[spl.Field], tree: LogicalPlan) =
    // TODO: replace the logic with select(ctx, tree, ne)?...
    Project(fields.map {
      case spl.Field(value) => UnresolvedAttribute(value)
    }, tree)

  private def applyRename(ctx: LogicalContext, tree: LogicalPlan, aliases: Seq[spl.Alias]): LogicalPlan = {
    val aliasMap = aliases.map(a => (attr(a.expr).name -> a)).toMap

    if (ctx.output.isEmpty)
      ctx.output = aliases.map(alias => attr(alias.expr))

    Project(ctx.output.map(item =>
      aliasMap.get(item.name) match {
        case Some(alias) => Alias(attr(alias.expr), alias.name)()
        case _ => item
      }
    ), tree)
  }

  // https://docs.splunk.com/Documentation/Splunk/8.2.2/SearchReference/Commontimeformatvariables
  // https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
  // and fix whatever is missing...
  // UNSUPPORTED: %V and %U (week of the year), %w (weekday as decimal), %k, %s	(unix epoch), and others
  private val stftimeToDateFormat = Map(
    "%Y" -> "yyyy",
    "%y" -> "yy",
    "%m" -> "MM",
    "%b" -> "MMM",
    "%B" -> "MMMM",
    "%d" -> "dd",
    "%A" -> "EEEE",
    "%a" -> "EE",
    "%e" -> "d",
    "%j" -> "D",
    "%H" -> "HH",
    "%l" -> "hh",
    "%M" -> "mm",
    "%S" -> "ss",
    "%p" -> "a",
    "%T" -> "HH:mm:ss",
    "%Z" -> "zz",
    "%%" -> "%"
  )

  private def attrOrExpr(ctx: LogicalContext, expr: spl.Expr): Expression =
    expr match {
      case field: spl.Field => ctx.splFieldToAttr(field)
      case _ => expression(ctx,expr)
  }

  private def attr(expr: spl.Expr): UnresolvedAttribute = expr match {
    case spl.Field(value) => UnresolvedAttribute(Seq(value))
    // TODO: support wildcards somehow...
    // TODO: failure mode
  }

  private def timeSpan(ts: spl.TimeSpan): Literal = {
    val utf8String = UTF8String.fromString(s"${ts.value} ${ts.scale}")
    val iv = IntervalUtils.stringToInterval(utf8String)
    Literal.create(iv)
  }

  private def snapTime(snap: String, time: Expression): TruncTimestamp = {
    val format = snap.replaceAll("s$", "")
    TruncTimestamp(Literal.create(format), time)
  }

  // https://docs.splunk.com/Documentation/SCS/current/Search/Timemodifiers
  private def relativeTime(expr: spl.Expr): Expression = expr match {
    case spl.Field("now") => Now()
    case ts: spl.TimeSpan => Add(Now(), timeSpan(ts))
    case spl.SnapTime(relative, snap, offset) =>
      val truncated = snapTime(snap, relative match {
        case Some(span) => Add(Now(), timeSpan(span))
        case None => Now()
      })
      offset match {
        case Some(value) => Add(truncated, timeSpan(value))
        case None => truncated
      }
    case _ => throw new AnalysisException(s"Not a relative time: $expr")
  }

  private def expression(ctx: LogicalContext, expr: spl.Expr): Expression = expr match {
    case constant: spl.Constant => mapConstants(constant)
    case call: spl.Call => function(ctx,call)
    case spl.Unary(symbol, right) => symbol match {
      case spl.UnaryNot => Not(expression(ctx,right))
      // TODO: failure modes
    }
    case spl.FieldIn(field, exprs) =>
      In(UnresolvedAttribute(field), exprs.map(expression(ctx, _)))
    case spl.Binary(left, spl.Equals, spl.Wildcard(pattern)) =>
      like(ctx, left, pattern)
    case spl.Binary(left, spl.NotEquals, spl.Wildcard(pattern)) =>
      Not(like(ctx, left, pattern))
    case spl.Binary(spl.Field("earliest"), spl.Equals, expr) =>
      val timeColumn = "_time"
      GreaterThanOrEqual(UnresolvedAttribute(timeColumn), relativeTime(expr))
    case spl.Binary(spl.Field("_index_earliest"), spl.Equals, expr) =>
      val timeColumn = "_time"
      GreaterThanOrEqual(UnresolvedAttribute(timeColumn), relativeTime(expr))
    case spl.Binary(spl.Field("latest"), spl.Equals, expr) =>
      val timeColumn = "_time"
      LessThanOrEqual(UnresolvedAttribute(timeColumn), relativeTime(expr))
    case spl.Binary(spl.Field("_index_latest"), spl.Equals, expr) =>
      val timeColumn = "_time"
      LessThanOrEqual(UnresolvedAttribute(timeColumn), relativeTime(expr))
    case spl.Binary(left, symbol, right) => symbol match {
      case straight: spl.Straight => straight match {
        case relational: spl.Relational => relational match {
          case spl.LessThan => LessThan(attrOrExpr(ctx, left), expression(ctx, right))
          case spl.GreaterThan => GreaterThan(attrOrExpr(ctx, left), expression(ctx, right))
          case spl.GreaterEquals => GreaterThanOrEqual(attrOrExpr(ctx, left), expression(ctx, right))
          case spl.LessEquals => LessThanOrEqual(attrOrExpr(ctx, left), expression(ctx, right))
          case spl.Equals => EqualTo(attrOrExpr(ctx, left), expression(ctx, right))
          case spl.NotEquals => Not(EqualTo(attrOrExpr(ctx, left), expression(ctx, right)))
        }
        case spl.Or => Or(expression(ctx, left), expression(ctx, right))
        case spl.And => And(expression(ctx, left), expression(ctx, right))
        case spl.Add => Add(expression(ctx, left), expression(ctx, right))
        case spl.Subtract => Subtract(expression(ctx, left), expression(ctx, right))
        case spl.Multiply => Multiply(expression(ctx, left), expression(ctx, right))
        case spl.Divide => Divide(expression(ctx, left), expression(ctx, right))
        case spl.Concatenate => Concat(Seq(expression(ctx, left), expression(ctx, right)))
        // TODO: make a failure case
      }
    }
    case _ => throw new AnalysisException(s"Cannot translate $expr")
  }

  private def like(ctx: LogicalContext, left: spl.Expr, pattern: String): Like = {
    val regex = Literal.create(pattern.replaceAll("\\*", "%"))
    Like(attrOrExpr(ctx, left), regex, '\\')
  }

  private def mapConstants(constant: spl.Constant): Literal = constant match {
    case spl.Null() => Literal.create(null)
    case spl.Bool(value) => Literal.create(value)
    case spl.Field(value) => Literal.create(value)
    case spl.StrValue(value) => Literal.create(value)
    case spl.IntValue(value) => Literal.create(value)
    case spl.DoubleValue(value) => Literal.create(value)
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
      case (spl.Field(value), order) =>
        SortOrder(UnresolvedAttribute(value), order)
    }
  }

  private def applyFields(ctx: LogicalContext,
                          tree: LogicalPlan,
                          removeFields: Boolean,
                          fields: Seq[spl.Field]): LogicalPlan =
    if (removeFields) {
      val columns = fields.map(_.value)
      selectColumns(ctx, tree, ctx.output.filter(item => !columns.contains(item.name)))
    }
    else
      selectExpr(fields, tree)

  private def applyRex(ctx: LogicalContext, tree: LogicalPlan, rc: spl.RexCommand): LogicalPlan =
  // TODO find a way to implement max_match, offset_field and mode
    rc.mode match {
      case Some(value) =>
        throw new NotImplementedError(s"rex mode=$value currently not supported!")
      case None =>
        val raw = selectColumn(ctx, tree, UnresolvedAttribute(ctx.rawFieldName))
        rexParseNamedGroup(rc.regex).foldLeft(raw) {
          case (plan, (colName, groupIndex)) =>
            withColumn(ctx, plan, colName, RegExpExtract(rc.field match {
              case Some(value) => UnresolvedAttribute(value)
              case None => UnresolvedAttribute(ctx.rawFieldName)
            }, Literal(rc.regex), Literal(groupIndex)))
        }
    }

  private def applyJoin(ctx: LogicalContext, tree: LogicalPlan, jc: spl.JoinCommand) = {
    val right = pipeline(ctx.copy(output = Seq()), jc.subSearch)
    Join(tree, right,
      UsingJoin(jc.joinType match {
        case "inner" => Inner
        case "left" => LeftOuter
        case "outer" => LeftOuter
        case _ => Inner
      }, jc.fields.map(_.value)), None, JoinHint.NONE)
  }

  private def applyRegex(ctx: LogicalContext,
                         tree: LogicalPlan,
                         item: Option[(spl.Field, String)],
                         regex: String) =
    item match {
      case Some((spl.Field(field), exclude)) =>
        val catalystOp = if (exclude.contains("!")) Not else (expr: Expression) => expr
        Filter(catalystOp(RLike(Column(field).expr, Literal(regex))), tree)
      case None =>
        Filter(RLike(Column(ctx.rawFieldName).expr, Literal(regex)), tree)
    }

  private def applyReturn(ctx: LogicalContext,
                          tree: LogicalPlan,
                          count: spl.IntValue,
                          fieldsOrAliases: Seq[spl.FieldOrAlias]) = {
    Limit(Literal(count.value), Project(fieldsOrAliases.map {
      case field: spl.Field => UnresolvedAttribute(field.value)
      case alias: spl.Alias => Alias(attr(alias.expr), alias.name)()
    }, tree))
  }

  private def applyEventStats(ctx: LogicalContext,
                              tree: LogicalPlan,
                              params: Map[String, String],
                              funcs: Seq[spl.Expr],
                              by: Seq[spl.Field] = Seq()): LogicalPlan = {
    // TODO implement allnum option
    val partitionSpec = by.map(attr)
    val sortOrderSpec = sortOrder(by.map(field => (Some("+"), field)))
    determineWindowStats(ctx, tree, funcs, partitionSpec, sortOrderSpec, UnspecifiedFrame)
  }

  private def applyStreamStats(ctx: LogicalContext,
                               tree: LogicalPlan,
                               funcs: Seq[spl.Expr],
                               by: Seq[spl.Field],
                               includeCurrentRow: Boolean,
                               wLength: Int): LogicalPlan = {
    val partitionSpec = by.map(attr)
    val sortOrderSpec = sortOrder(Seq((Some("+"), spl.Field(ctx.timeFieldName))))
    if (wLength < 0) {
      throw new AnalysisException(s"window parameter can't be negative: $wLength")
    }
    val wUpper = Literal(if (includeCurrentRow) 0 else -1)
    val wLower = if (wLength > 0) Subtract(wUpper, Literal(wLength - 1)) else UnboundedPreceding
    val wFrame = SpecifiedWindowFrame(RowFrame, wLower, wUpper)
    determineWindowStats(ctx, tree, funcs, partitionSpec, sortOrderSpec, wFrame)
  }

  private def determineWindowStats(ctx: LogicalContext,
                                   tree: LogicalPlan,
                                   funcs: Seq[spl.Expr],
                                   partitionSpec: Seq[Expression],
                                   sortOrderSpec: Seq[SortOrder],
                                   windowFrame: WindowFrame): LogicalPlan = {
    val windowSpec = WindowSpecDefinition(partitionSpec, sortOrderSpec, windowFrame)
    funcs.foldLeft(tree) {
      case (plan, spl.Alias(expr, name)) =>
        withColumn(ctx, plan, name, WindowExpression(expression(ctx, expr), windowSpec))
      case (plan, expr: spl.Expr) =>
        withColumn(ctx, plan,
          // when no name/alias is passed to the spl command, spl generates default column name based on the expression
          // ie. `eventstats min(column) ...` will create a column named "min(column)"
          expression(ctx, expr).toString.replace("'", ""),
          WindowExpression(expression(ctx, expr), windowSpec))
    }
  }

  /**
   * TODO: For sorting we need to check that the Lexicographical order in Splunk is the same in Spark
   * https://docs.splunk.com/Documentation/Splunk/8.2.2/SearchReference/Dedup
   */
  private def getSortByWindowExpr(fields: Seq[spl.Field], cmd: spl.SortCommand): WindowExpression =
    WindowExpression(RowNumber(), WindowSpecDefinition(
      partitionSpec = fields.map(attr),
      orderSpec = sortOrder(cmd.fieldsToSort),
      frameSpecification = UnspecifiedFrame)
    )

  private def applyDedup(ctx: LogicalContext, tree: LogicalPlan, cmd: spl.DedupCommand) = {
    val windowExpr = getSortByWindowExpr(cmd.fields, cmd.sortBy)
    Project(ctx.output, Filter(
      LessThanOrEqual(UnresolvedAttribute("_rn"), Literal(cmd.numResults)),
      withColumn(ctx,
        withColumn(ctx, tree, "_no", MonotonicallyIncreasingID()),
        "_rn", windowExpr)
      )
    )
  }

  private def applyInputLookup(ctx: LogicalContext, plan: LogicalPlan, iLookup: spl.InputLookup) =
    // TODO implement `append`, `strict` and `start` options, also inputlookup should accept file name
    Limit(
      Literal(iLookup.max),
      iLookup.where match {
        case Some(where) => Filter(expression(ctx, where), plan)
        case _ => plan
      })

  private def applyFormat(ctx: LogicalContext,
                          tree: LogicalPlan,
                          fc: spl.FormatCommand): LogicalPlan = {
    // TODO Implement behaviour with mvsep when multiple value field is present
    val existingColumnNames = ctx.output.map(_.name)
    val colLevelPattern = existingColumnNames.map(column =>
      s"${fc.colPrefix}${column}=%s${fc.colEnd}"
    )
    val rowLevelPattern = fc.rowPrefix + colLevelPattern.mkString(s" ${fc.colSep} ") + fc.rowEnd
    newAggregateIgnoringABI(Nil, Seq(
      Alias(
        ArrayJoin(
          AggregateExpression(
            CollectList(
              FormatString((Literal(rowLevelPattern) +: ctx.output): _*)
            ), Complete, isDistinct = false
          ), Literal(s" ${fc.rowSep} "), None
        ), "search")()
    ), fc.maxResults match {
      case 0 => tree
      case _ => Limit(Literal(fc.maxResults), tree)
    })
  }

  private def applyMvCombine(ctx: LogicalContext,
                             tree: LogicalPlan,
                             field: spl.Field,
                             delim: Option[String]) = {
    val grpExprs = ctx.output.filter(!_.name.equals(field.value))
    val aggExpr = AggregateExpression(
      CollectList(UnresolvedAttribute(field.value)),
      Complete,
      isDistinct = false
    )
    newAggregateIgnoringABI(
      grpExprs,
      grpExprs ++ Seq(
        Alias(
          delim match {
            case Some(delimValue) => ArrayJoin(aggExpr, Literal(delimValue), None)
            case _ => aggExpr
          }, field.value)(),
      ), tree
    )
  }

  private def applyMvExpand(ctx: LogicalContext, tree: LogicalPlan, field: spl.Field, limit: Option[Int]) = {
    val attribute = attr(field)
    val expr = if(limit isEmpty) attribute else Slice(attribute, Literal(1), Literal(limit.get))
    val explodedAttr = Explode(expr)
    withColumn(ctx, tree, field.value, explodedAttr)
  }

  private def applyBin(ctx: LogicalContext, tree: LogicalPlan, bc: spl.BinCommand) = {
    val (field, alias) = bc.field match {
      case spl.Field(v) => (UnresolvedAttribute(v), v)
      case spl.Alias(spl.Field(v), alias) => (UnresolvedAttribute(v), alias)
    }
    if (bc.span.isEmpty) {
      throw new AnalysisException(s"Currently, only `span` is implemented: $bc")
    }
    val ts = bc.span.get.asInstanceOf[spl.TimeSpan]
    val duration = Literal.create(timeSpan(ts).toString())
    // technically, we can do it in one stage, but generated code would be less readable
    val projectWindow = withColumn(ctx, tree, alias, new TimeWindow(field, duration))
    withColumn(ctx, projectWindow, alias, UnresolvedAttribute(Seq(alias, "start")))
  }
}

case class UnknownPlanShim(t: String, child: LogicalPlan) extends LogicalPlan {
  override def output: Seq[Attribute] = child.output
  override def children: Seq[LogicalPlan] = child.children
}