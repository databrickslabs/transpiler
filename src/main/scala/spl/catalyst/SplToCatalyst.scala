package spl.catalyst

import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.control.NonFatal
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{CidrMatch, FillNullShim, Term}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, UsingJoin}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.unsafe.types.UTF8String
import spl.ast

object SplToCatalyst extends Logging {
  def pipeline(ctx: LogicalContext, p: ast.Pipeline): LogicalPlan = {
    val (table, pipe) = p.commands.head match {
      case _: ast.MakeResults => (null, p)
      case _ => determineTable(ctx, p)
    }
    pipe.commands.foldLeft(table) {
      wrapCommand(ctx) {
        (tree, command) => command match {
          case ast.SearchCommand(expr) =>
            // probably search is different from where...
            Filter(expression(ctx, expr), tree)

          case inputLookup: ast.InputLookup =>
            // TODO implement `append`, `strict` and `start` options,
            //  also inputlookup should accept file name
            applyInputLookup(ctx, tree, inputLookup)

          case ast.WhereCommand(expr) =>
            Filter(expression(ctx, expr), tree)

          case ast.EvalCommand(fields) =>
            fields.foldLeft(tree) { (plan, field) =>
              val (ast.Field(name), expr) = field
              withColumn(ctx, plan, name, expr)
            }

          case ast.TableCommand(fields) =>
            selectExpr(fields, tree)

          case ast.ConvertCommand(timeformat, convs) =>
            // TODO add support for wildcard fields
            convs.foldLeft(tree) { (plan, fc) =>
              val name = fc.alias.getOrElse(fc.field).value
              val callArgs = Seq(fc.field, ast.StrValue(timeformat))
              withColumn(ctx, plan, name, ast.Call(fc.func, callArgs))
            }

          case ast.HeadCommand(expr, keepLast, nullOption) =>
            // TODO Implement keeplast and null options behaviour
            log.debug(s"Adding `HeadCommand` with options: $expr to the tree")
            if (expr.isInstanceOf[ast.IntValue]) Limit(expression(ctx, expr), tree)
            else Filter(expression(ctx, expr), tree)

          case ast.SortCommand(fields) =>
            Sort(sortOrder(fields), global = true, tree)

          case ast.FieldsCommand(rmFields, fields) =>
            applyFields(ctx, tree, rmFields, fields)

          case ast.LookupCommand(dataset, fields, output) =>
            leftJoinUsing(ctx, dataset, fields, output, tree)

          case cc: ast.CollectCommand =>
            // fields.map(fieldName => Column(fieldName.value))
            // TODO: add projection if fields is not empty
            AppendData(UnresolvedRelation(Seq(cc.index)), tree, Map(), isByName = true)

          case st: ast.StatsCommand =>
            val agg = aggregate(ctx, st.by, st.funcs, tree)
            if (st.dedupSplitVals) {
              Deduplicate(st.by.map(x => UnresolvedAttribute(x.value)), agg)
            } else agg

          case rc: ast.RexCommand =>
            // TODO find a way to implement max_match, offset_field and mode
            applyRex(ctx, tree, rc)

          case ast.RenameCommand(aliases) =>
            applyRename(ctx, tree, aliases)

          case ast.RegexCommand(item, regex) =>
            applyRegex(ctx, tree, item, regex)

          case jc: ast.JoinCommand =>
            applyJoin(ctx, tree, jc)

          case ast.ReturnCommand(count, fieldsOrAliases) =>
            applyReturn(ctx, tree, count, fieldsOrAliases)

          case ast.FillNullCommand(value, fields) =>
            val fieldsOpt = fields.getOrElse(Seq.empty[ast.Field]).map(_.value).toSet
            FillNullShim(value.getOrElse("0"), fieldsOpt, tree)

          case ast.EventStatsCommand(_, funcs, by) =>
            // TODO implement allnum option
            applyEventStats(ctx, tree, funcs, by)

          case ast.StreamStatsCommand(funcs, by, current, window) =>
            applyStreamStats(ctx, tree, funcs, by, current, window)

          case dedupCmd: ast.DedupCommand =>
            assertCtxOutputNonEmpty(ctx, dedupCmd)
            applyDedup(ctx, tree, dedupCmd)

          case fc: ast.FormatCommand =>
            // TODO Implement behaviour with mvsep when multiple value field is present
            assertCtxOutputNonEmpty(ctx, fc)
            applyFormat(ctx, tree, fc)

          case mv: ast.MvCombineCommand =>
            assertCtxOutputNonEmpty(ctx, mv)
            applyMvCombine(ctx, tree, mv.field, mv.delim)

          case ast.MvExpandCommand(field, limit) =>
            applyMvExpand(ctx, tree, field, limit)

          case bc: ast.BinCommand =>
            applyBin(ctx, tree, bc)

          case mr: ast.MakeResults =>
            applyMakeResults(ctx, mr)

          case at: ast.AddTotals =>
            applyAddTotals(ctx, tree, at)
        }
      }
    }
  }

  private def function(ctx: LogicalContext, call: ast.Call): Expression = call.name match {
    case "isnull" =>
      IsNull(attrOrExpr(ctx, call.args.head))
    case "if" =>
      val branches = Seq((attrOrExpr(ctx, call.args.head), attrOrExpr(ctx, call.args(1))))
      val elseValue = Some(attrOrExpr(ctx, call.args(2)))
      CaseWhen(branches, elseValue)
    case "cidrmatch" =>
      val cidr = attrOrExpr(ctx, call.args.head)
      val ip = attrOrExpr(ctx, call.args(1))
      callCidrMatch(ctx, cidr, ip)
    case "ctime" =>
      determineTimeConversion(ctx, call)
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
      Substring(str, pos, len)
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
      determineTimeConversion(ctx, call)
    case "mvcount" =>
      Size(attrOrExpr(ctx, call.args.head))
    case "mvindex" =>
      val mvfield = attrOrExpr(ctx, call.args.head)
      val start = extractIndex(call.args(1))
      val stop = call.args.lift(2).map(extractIndex(_)).getOrElse(start)
      if (start * stop < 0) {
        throw new ConversionFailure(s"A combination of negative and positive " +
          s"start and stop indices is not supported. start_index: $start stop_index: $stop")
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
        throw new ConversionFailure(s"Expression references more than one field: $fields")
      }
      mvFilter(ctx, fields.head, expr)
    case "null" =>
      Literal(null)
    case "isnotnull" =>
      IsNotNull(attrOrExpr(ctx, call.args.head))
    case "memk" =>
      val field = attrOrExpr(ctx, call.args.head)
      callMemk(ctx, field)
    case "rmunit" =>
      val field = attrOrExpr(ctx, call.args.head)
      callRmUnit(ctx, field)
    case "rmcomma" =>
      val field = attrOrExpr(ctx, call.args.head)
      callRmComma(ctx, field)
    case "num" =>
      callNum(ctx, call)
    case _ =>
      val approx = s"${call.name}(${call.args.map(_.toString).mkString(",")})"
      throw new ConversionFailure(s"Unknown SPL function: $approx")
  }

  private def assertCtxOutputNonEmpty(ctx: LogicalContext, command: ast.Command): Unit = {
    if (ctx.output.isEmpty) {
      throw EmptyContextOutput(command)
    }
  }

  private def callNum(ctx: LogicalContext, call: ast.Call): Expression = {
    val field = attrOrExpr(ctx, call.args.head)
    CaseWhen(Seq(
      (IsNotNull(determineTimeConversion(ctx, call)), determineTimeConversion(ctx, call)),
      (IsNotNull(Cast(field, DoubleType)), Cast(field, DoubleType)),
      (IsNotNull(callMemk(ctx, field)), callMemk(ctx, field)),
      (IsNotNull(callRmUnit(ctx, field)), callRmUnit(ctx, field)),
      (IsNotNull(callRmComma(ctx, field)), callRmComma(ctx, field))
    ), None)
  }

  private def callRmComma(ctx: LogicalContext, field: Expression): Expression = {
    Cast(RegExpReplace(field, Literal.create(","), Literal.create("")), DoubleType)
  }

  private def callRmUnit(ctx: LogicalContext, field: Expression): Expression = {
    val regex = Literal.create("(?i)^(\\d*\\.?\\d+)(\\w*)$")
    Cast(RegExpExtract(field, regex, Literal.create(1)), DoubleType)
  }

  private def callMemk(ctx: LogicalContext, field: Expression): Expression = {
    val regex = Literal.create("(?i)^(\\d*\\.?\\d+)([kmg])$")
    val size = Cast(RegExpExtract(field, regex, Literal.create(1)), DoubleType)
    val format = Upper(RegExpExtract(field, regex, Literal.create(2)))
    val multiplier = CaseWhen(Seq(
      (EqualTo(format, Literal.create("K")), Literal.create(1.0)),
      (EqualTo(format, Literal.create("M")), Literal.create(1024.0)),
      (EqualTo(format, Literal.create("G")), Literal.create(1024.0 * 1024.0))
    ), Literal.create(1.0))
    Multiply(size, multiplier)
  }

  private def callCidrMatch(ctx: LogicalContext, cidr: Expression, ip: Expression): Expression = {
    ip match {
      case str: Literal => CidrMatch(cidr, str)
      case attr: UnresolvedAttribute =>
        val ipRef = ctx.output.filter(e => e.name.equals(attr.name)).headOption.getOrElse(attr)
        CidrMatch(cidr, ipRef)
      case _ => throw new ConversionFailure(s"ip must be String or Field: ${ip.toString()}")
    }
  }

  private def determineMin(ctx: LogicalContext, call: ast.Call): Expression = {
    call.args match {
      case Seq(ast.Field(v)) =>
        AggregateExpression(
          Min(attrOrExpr(ctx, call.args.head)),
          Complete, isDistinct = false)
      case Seq(_, _*) =>
        Least(call.args.map(attrOrExpr(ctx, _)))
    }
  }

  private def determineMax(ctx: LogicalContext, call: ast.Call): Expression = {
    call.args match {
      case Seq(ast.Field(v)) =>
        AggregateExpression(
          Max(attrOrExpr(ctx, call.args.head)),
          Complete, isDistinct = false)
      case Seq(_, _*) =>
        Greatest(call.args.map(attrOrExpr(ctx, _)))
    }
  }

  private def determineTimeConversion(ctx: LogicalContext, call: ast.Call): Expression = {
    DateFormatClass(attrOrExpr(ctx, call.args.head), Literal.create(call.args.lift(1) match {
      case Some(ast.Field(fmt)) => stftimeToDateFormat.foldLeft(fmt) {
        case (a, (b, c)) => a.replaceAll(b, c)
      }
      case Some(ast.StrValue(fmt)) => stftimeToDateFormat.foldLeft(fmt) {
        case (a, (b, c)) => a.replaceAll(b, c)
      }
      case _ => throw new ConversionFailure(s"Invalid strftime format given")
    }))
  }

  private def mvFilter(ctx: LogicalContext, field: ast.Field, expr: ast.Expr) = {
    val filterArgument = attrOrExpr(ctx, field)
    val lctx = ctx.copy(splFieldToAttr = field => UnresolvedNamedLambdaVariable(Seq(field.value)))
    val filterFunction = LambdaFunction(expression(lctx, expr), Seq(
      UnresolvedNamedLambdaVariable(Seq(field.value))))
    ArrayFilter(filterArgument, filterFunction)
  }

  private def extractFields(expr: ast.Expr): Set[ast.Field] = expr match {
    case constant: ast.Constant => constant match {
      case field: ast.Field => Set(field)
      case _ => Set()
    }
    case ast.Unary(_, right) => extractFields(right)
    case ast.Binary(left, _, right) => extractFields(left)++extractFields(right)
    case ast.Call(_, args) => args.flatMap(extractFields).toSet
    case ast.FieldIn(_, exprs) => exprs.flatMap(extractFields).toSet
    case _ => Set()
  }

  private def extractIndex(x: ast.Expr, offset: Int = 1) : Int = x match {
    case ast.IntValue(index) => if (index >= 0) index + offset else index
    case _ => throw new ConversionFailure("int expected")
  }

  private def isFilter(x: ast.Expr, name: String) = x match {
    case ast.Binary(ast.Field(field), ast.Equals, _) if field.equals(name) => true
    case _ => false
  }

  /** Finds indices in all of the binary nodes */
  private def findIndices(search: ast.Expr): Set[String] = search match {
    case b @ ast.Binary(_, _, ast.Field(value)) if isFilter(b, "index") => Set(value)
    case ast.Binary(left, _, right) => findIndices(left) ++ findIndices(right)
    case _ => Set()
  }

  /** Removes `index` filters, as they are lifted to the top of the tree */
  private def overwriteSplSearch(x: ast.Expr): ast.Expr = x match {
    case ast.Binary(left, ast.And, right) if isFilter(left, "index") => right
    case ast.Binary(left, ast.And, right) if isFilter(right, "index") => left
    // TODO: modify "earliest" filter
    case ast.Binary(left, symbol, right) => ast.Binary(overwriteSplSearch(left),
      symbol, overwriteSplSearch(right))
    case y: ast.Expr => y
  }

  /**
   * Index maps directly on Spark's table. Any filters on index are removed.
   * If no index is specified, the value is taken from the context
   */
  private def determineTable(ctx: LogicalContext, p: ast.Pipeline): (LogicalPlan, ast.Pipeline) = {
    val indices = p.commands.flatMap {
      case ast.SearchCommand(expr) => findIndices(expr)
      case inputLookup: ast.InputLookup => Set(inputLookup.tableName)
      case _ => Seq()
    }
    if (indices.size > 1) {
      throw new ConversionFailure(s"Only one index allowed: ${indices.mkString(",")}")
    }
    val tableName = indices.headOption.getOrElse(ctx.indexName)
    val table = UnresolvedRelation(Seq(tableName)).asInstanceOf[LogicalPlan]
    ctx.output ++= ctx.analyzePlan(table)
    // `filterNot` is being used here to filter out a `SearchCommand` that only contains `index`.
    // The idea is to remove the `index` filters, as they are lifted to the top of the tree
    (table, p.copy(commands = p.commands.filterNot {
      case s: ast.SearchCommand => isFilter(s.expr, "index")
      case _ => false
    }.map {
      case s: ast.SearchCommand => s.copy(expr = overwriteSplSearch(s.expr))
      case c: ast.Command => c
    }
    ))
  }

  private def leftJoinUsing(ctx: LogicalContext, dataset: String,
                            fields: Seq[ast.FieldLike],
                            output: Option[ast.LookupOutput],
                            tree: LogicalPlan): Join = {
    val hasAliases: Boolean = fields exists {
      case _: ast.Field => false
      case _: ast.Alias => true
      case _: ast.AliasedField => true
      case _ => false
    }
    var right = UnresolvedRelation(Seq(dataset)).asInstanceOf[LogicalPlan]
    if (hasAliases || output.isDefined) {
      // TODO: modify LogicalContext output
      right = Project(fields.map(fieldOrAlias(ctx, _)) ++ (output match {
        case Some(ast.LookupOutput(kv, outputFields)) =>
          outputFields.map(fieldOrAlias(ctx, _))
        case None => Seq()
      }), right)
    }
    Join(tree, right,
      UsingJoin(LeftOuter, fields.map {
        case ast.Field(fieldName) => fieldName
        case ast.AliasedField(_, alias) => alias
        case _ => "unknown"
      }), None, JoinHint.NONE)
  }

  private def fieldOrAlias(ctx: LogicalContext, field: ast.FieldLike): NamedExpression =
    field match {
      case ast.Field(fieldName) =>
        UnresolvedAttribute(fieldName)
      case ast.AliasedField(ast.Field(fieldName), alias) =>
        Alias(UnresolvedAttribute(fieldName), alias)()
      case ast.Alias(expr, alias) =>
        Alias(expression(ctx, expr), alias)()
      case ast.Wildcard(value) =>
        throw new ConversionFailure(s"cannot alias a wildcard yet: $value")
    }

  private def aggregate(ctx: LogicalContext, by: Seq[ast.Field], funcs: Seq[ast.Expr],
                        tree: LogicalPlan) = {
    // TODO: select _time
    val groupBy = by.map(attr)
    val agg = aggregates(ctx, funcs)
    val child = if (hasTimeFunctions(funcs)) Sort(Seq(
      SortOrder(UnresolvedAttribute(ctx.timeFieldName), Ascending, NullsFirst, Seq.empty)
    ), global = true, tree) else tree
    // TODO: hm... ctx.output = Seq()
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
                                      child: LogicalPlan): Aggregate =
    aggregateConstructorArgCountABI match {
      case 3 => Aggregate(groupingExpressions, aggregateExpressions, child)
      case 4 => aggregateConstructorReflection
        .newInstance(groupingExpressions, aggregateExpressions, child, None)
        .asInstanceOf[Aggregate]
      case _ => throw new ConversionFailure(s"Incompatible runtime detected")
    }

  private val (aggregateConstructorReflection, aggregateConstructorArgCountABI) = {
    val firstConstructor = classOf[Aggregate].getConstructors.head
    (firstConstructor, firstConstructor.getParameterCount)
  }

  private def hasTimeFunctions(funcs: Seq[ast.Expr]) : Boolean = funcs.map {
    case ast.Call(name, _) => name
    case ast.Alias(ast.Call(name, _), _) => name
    case _ => ""
  } exists(Seq("earliest", "latest").contains(_))

  private def aggregates(ctx: LogicalContext, funcs: Seq[ast.Expr]): Seq[NamedExpression] =
    funcs.map {
      case call: ast.Call =>
        Alias(function(ctx, call), call.name)()
      case ast.Alias(call: ast.Call, name) =>
        Alias(function(ctx, call), name)()
      case x: ast.Expr =>
        // scalastyle:off
        throw new NotImplementedError(s"cannot convert aggregate: $x")
        // scalastyle:on
    }

  private def withColumn(ctx: LogicalContext,
                         tree: LogicalPlan,
                         name: String,
                         expr: ast.Expr): Project =
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
                         (mapper: (LogicalPlan, ast.Command) => LogicalPlan)
                         (plan: LogicalPlan, command: ast.Command): LogicalPlan =
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

  private def selectExpr(fields: Seq[ast.Field], tree: LogicalPlan) =
  // TODO: replace the logic with select(ctx, tree, ne)?...
    Project(fields.map {
      case ast.Field(value) => UnresolvedAttribute(value)
    }, tree)

  private def applyRename(ctx: LogicalContext, tree: LogicalPlan,
                          aliases: Seq[ast.Alias]): LogicalPlan = {
    val aliasMap = aliases.map(a => attr(a.expr).name -> a).toMap
    if (ctx.output.isEmpty) {
      ctx.output = aliases.map(alias => attr(alias.expr))
    }
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
  // UNSUPPORTED: %V, %U, %w, %k, %s, ...
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

  private def attrOrExpr(ctx: LogicalContext, expr: ast.Expr): Expression =
    expr match {
      case field: ast.Field => ctx.splFieldToAttr(field)
      case _ => expression(ctx, expr)
    }

  private def attr(expr: ast.Expr): UnresolvedAttribute = expr match {
    case ast.Field(value) => UnresolvedAttribute(Seq(value))
    case a: Any => throw new ConversionFailure(s"Cannot attribute from $a")
  }

  private def timeSpan(ts: ast.TimeSpan): Literal = {
    val utf8String = UTF8String.fromString(s"${ts.value} ${ts.scale}")
    val iv = IntervalUtils.stringToInterval(utf8String)
    Literal.create(iv)
  }

  private def snapTime(snap: String, time: Expression): TruncTimestamp = {
    val format = snap.replaceAll("s$", "")
    TruncTimestamp(Literal.create(format), time)
  }

  // https://docs.splunk.com/Documentation/SCS/current/Search/Timemodifiers
  private def relativeTime(expr: ast.Expr): Expression = expr match {
    case ast.Field("now") => Now()
    case ts: ast.TimeSpan => Add(Now(), timeSpan(ts))
    case ast.SnapTime(relative, snap, offset) =>
      val truncated = snapTime(snap, relative match {
        case Some(span) => Add(Now(), timeSpan(span))
        case None => Now()
      })
      offset match {
        case Some(value) => Add(truncated, timeSpan(value))
        case None => truncated
      }
    case _ => throw new ConversionFailure(s"Not a relative time: $expr")
  }

  private def expression(ctx: LogicalContext, expr: ast.Expr): Expression = expr match {
    case constant: ast.Constant => mapConstants(constant)
    case call: ast.Call => function(ctx, call)
    case ast.Unary(symbol, right) => symbol match {
      case ast.UnaryNot => Not(expression(ctx, right))
      case _ => throw new ConversionFailure(s"unsupported unary: $symbol")
    }
    case ast.FieldIn(field, exprs) =>
      In(UnresolvedAttribute(field), exprs.map(expression(ctx, _)))
    case ast.Binary(left, ast.Equals, ast.Wildcard(pattern)) =>
      like(ctx, left, pattern)
    case ast.Binary(left, ast.NotEquals, ast.Wildcard(pattern)) =>
      Not(like(ctx, left, pattern))
    case ast.Binary(ast.Field("earliest"), ast.Equals, expr) =>
      GreaterThanOrEqual(UnresolvedAttribute(ctx.timeFieldName), relativeTime(expr))
    case ast.Binary(ast.Field("_index_earliest"), ast.Equals, expr) =>
      GreaterThanOrEqual(UnresolvedAttribute(ctx.timeFieldName), relativeTime(expr))
    case ast.Binary(ast.Field("latest"), ast.Equals, expr) =>
      LessThanOrEqual(UnresolvedAttribute(ctx.timeFieldName), relativeTime(expr))
    case ast.Binary(ast.Field("_index_latest"), ast.Equals, expr) =>
      LessThanOrEqual(UnresolvedAttribute(ctx.timeFieldName), relativeTime(expr))
    case ast.Binary(ast.Field(ip), ast.Equals, ast.IPv4CIDR(cidr)) =>
      callCidrMatch(ctx, Literal.create(cidr), UnresolvedAttribute(ip))
    case ast.Binary(left, symbol, right) => symbol match {
      case straight: ast.Straight => straight match {
        case relational: ast.Relational => relational match {
          case ast.LessThan =>
            LessThan(attrOrExpr(ctx, left), expression(ctx, right))
          case ast.GreaterThan =>
            GreaterThan(attrOrExpr(ctx, left), expression(ctx, right))
          case ast.GreaterEquals =>
            GreaterThanOrEqual(attrOrExpr(ctx, left), expression(ctx, right))
          case ast.LessEquals =>
            LessThanOrEqual(attrOrExpr(ctx, left), expression(ctx, right))
          case ast.Equals =>
            EqualTo(attrOrExpr(ctx, left), expression(ctx, right))
          case ast.NotEquals =>
            Not(EqualTo(attrOrExpr(ctx, left), expression(ctx, right)))
          case _ =>
            throw new ConversionFailure(s"unsupported relational: $relational")
        }
        case ast.Or => Or(attrOrExpr(ctx, left), attrOrExpr(ctx, right))
        case ast.And => And(attrOrExpr(ctx, left), attrOrExpr(ctx, right))
        case ast.Add => Add(attrOrExpr(ctx, left), attrOrExpr(ctx, right))
        case ast.Subtract => Subtract(attrOrExpr(ctx, left), attrOrExpr(ctx, right))
        case ast.Multiply => Multiply(attrOrExpr(ctx, left), attrOrExpr(ctx, right))
        case ast.Divide => Divide(attrOrExpr(ctx, left), attrOrExpr(ctx, right))
        case ast.Concatenate => Concat(Seq(attrOrExpr(ctx, left), attrOrExpr(ctx, right)))
        case _ => throw new ConversionFailure(s"unsupported binary: $symbol")
      }
      case _ => throw new ConversionFailure(s"unsupported symbol: $symbol")
    }
    case _ => throw new ConversionFailure(s"cannot translate $expr")
  }

  private def like(ctx: LogicalContext, left: ast.Expr, pattern: String): Like = {
    val regex = Literal.create(pattern.replaceAll("\\*", "%"))
    Like(attrOrExpr(ctx, left), regex, '\\')
  }

  private def mapConstants(constant: ast.Constant): Literal = constant match {
    case ast.Null() => Literal.create(null)
    case ast.Bool(value) => Literal.create(value)
    case ast.Field(value) => Literal.create(value)
    case ast.StrValue(value) => Literal.create(value)
    case ast.IntValue(value) => Literal.create(value)
    case ast.DoubleValue(value) => Literal.create(value)
    case ast.IPv4CIDR(value) => Literal.create(value)
    case _ => throw new ConversionFailure(s"constant $constant")
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

  private def sortOrder(fields: Seq[(Option[String], ast.Expr)]): Seq[SortOrder] = {
    fields map {
      case Tuple2(a, b) => (b, if (a.getOrElse("+") == "-") Descending else Ascending)
    } map {
      case (ast.Call(name, args), order) => name match {
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
      case (ast.Field(value), order) =>
        SortOrder(UnresolvedAttribute(value), order)
      case (e: ast.Expr, _) => throw new ConversionFailure(s"sort order: $e")
    }
  }

  private def applyFields(ctx: LogicalContext,
                          tree: LogicalPlan,
                          removeFields: Boolean,
                          fields: Seq[ast.Field]): LogicalPlan =
    if (removeFields) {
      val columns = fields.map(_.value)
      selectColumns(ctx, tree, ctx.output.filter(item => !columns.contains(item.name)))
    } else selectExpr(fields, tree)

  private def applyRex(ctx: LogicalContext, tree: LogicalPlan, rc: ast.RexCommand): LogicalPlan =
    // TODO find a way to implement max_match, offset_field and mode
    rc.mode match {
      case Some(value) =>
        // scalastyle:off
        throw new NotImplementedError(s"rex mode=$value currently not supported!")
        // scalastyle:on
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

  private def applyJoin(ctx: LogicalContext, tree: LogicalPlan, jc: ast.JoinCommand) = {
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
                         item: Option[(ast.Field, String)],
                         regex: String): Filter = item match {
      case Some((ast.Field(field), exclude)) =>
        val catalystOp = if (exclude.contains("!")) Not else (expr: Expression) => expr
        Filter(catalystOp(RLike(UnresolvedAttribute(field), Literal(regex))), tree)
      case None =>
        Filter(RLike(UnresolvedAttribute(ctx.rawFieldName), Literal(regex)), tree)
    }

  private def applyReturn(ctx: LogicalContext,
                          tree: LogicalPlan,
                          count: ast.IntValue,
                          fieldsOrAliases: Seq[ast.FieldOrAlias]) = {
    Limit(Literal(count.value), Project(fieldsOrAliases.map {
      case field: ast.Field => UnresolvedAttribute(field.value)
      case alias: ast.Alias => Alias(attr(alias.expr), alias.name)()
    }, tree))
  }

  private def applyEventStats(ctx: LogicalContext, tree: LogicalPlan, funcs: Seq[ast.Expr],
                              by: Seq[ast.Field]): LogicalPlan = {
    // TODO implement allnum option
    val partitionSpec = by.map(attr)
    val sortOrderSpec = sortOrder(by.map(field => (Some("+"), field)))
    determineWindowStats(ctx, tree, funcs, partitionSpec, sortOrderSpec, UnspecifiedFrame)
  }

  private def applyStreamStats(ctx: LogicalContext,
                               tree: LogicalPlan,
                               funcs: Seq[ast.Expr],
                               by: Seq[ast.Field],
                               includeCurrentRow: Boolean,
                               wLength: Int): LogicalPlan = {
    val partitionSpec = by.map(attr)
    val sortOrderSpec = sortOrder(Seq((Some("+"), ast.Field(ctx.timeFieldName))))
    if (wLength < 0) {
      throw new ConversionFailure(s"window parameter can't be negative: $wLength")
    }
    val wUpper = Literal(if (includeCurrentRow) 0 else -1)
    val wLower = if (wLength > 0) Subtract(wUpper, Literal(wLength - 1)) else UnboundedPreceding
    val wFrame = SpecifiedWindowFrame(RowFrame, wLower, wUpper)
    determineWindowStats(ctx, tree, funcs, partitionSpec, sortOrderSpec, wFrame)
  }

  private def determineWindowStats(ctx: LogicalContext,
                                   tree: LogicalPlan,
                                   funcs: Seq[ast.Expr],
                                   partitionSpec: Seq[Expression],
                                   sortOrderSpec: Seq[SortOrder],
                                   windowFrame: WindowFrame): LogicalPlan = {
    val windowSpec = WindowSpecDefinition(partitionSpec, sortOrderSpec, windowFrame)
    funcs.foldLeft(tree) {
      case (plan, ast.Alias(expr, name)) =>
        withColumn(ctx, plan, name, WindowExpression(expression(ctx, expr), windowSpec))
      case (plan, expr: ast.Expr) =>
        withColumn(ctx, plan,
          // when no name/alias is passed to the spl command, spl generates default column name
          // based on the expression: `eventstats min(column) ...` -> "min(column)"
          expression(ctx, expr).toString.replace("'", ""),
          WindowExpression(expression(ctx, expr), windowSpec))
    }
  }

  /**
   * https://docs.splunk.com/Documentation/Splunk/8.2.2/SearchReference/Dedup
   */
  private def getSortByWindowExpr(fields: Seq[ast.Field], cmd: ast.SortCommand): WindowExpression =
    WindowExpression(RowNumber(), WindowSpecDefinition(
      partitionSpec = fields.map(attr),
      orderSpec = sortOrder(cmd.fieldsToSort),
      frameSpecification = UnspecifiedFrame)
    )

  private def applyDedup(ctx: LogicalContext, tree: LogicalPlan, cmd: ast.DedupCommand) = {
    if (ctx.output.isEmpty) {
      ctx.output = cmd.fields.map(item => UnresolvedAttribute(item.value))
    }
    val windowExpr = getSortByWindowExpr(cmd.fields, cmd.sortBy)
    Project(ctx.output, Filter(
      LessThanOrEqual(UnresolvedAttribute("_rn"), Literal(cmd.numResults)),
      withColumn(ctx,
        withColumn(ctx, tree, "_no", MonotonicallyIncreasingID()),
        "_rn", windowExpr)
    )
    )
  }

  private def applyInputLookup(ctx: LogicalContext, plan: LogicalPlan, iLookup: ast.InputLookup) =
  // TODO implement `append`, `strict` and `start` options, also inputlookup should accept file name
    Limit(
      Literal(iLookup.max),
      iLookup.where match {
        case Some(where) => Filter(expression(ctx, where), plan)
        case _ => plan
      })

  private def applyFormat(ctx: LogicalContext,
                          tree: LogicalPlan,
                          fc: ast.FormatCommand): LogicalPlan = {
    // TODO Implement behaviour with mvsep when multiple value field is present
    val existingColumnNames = ctx.output.map(_.name)
    val colLevelPattern = existingColumnNames.map(column =>
      s"${fc.colPrefix}$column=%s${fc.colEnd}"
    )
    val rowLevelPattern = fc.rowPrefix + colLevelPattern.mkString(s" ${fc.colSep} ") + fc.rowEnd
    newAggregateIgnoringABI(Nil, Seq(
      Alias(
        ArrayJoin(
          AggregateExpression(
            CollectList(
              FormatString(Literal(rowLevelPattern) +: ctx.output: _*)
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
                             field: ast.Field,
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
          }, field.value)()
      ), tree
    )
  }

  private def applyMakeResults(ctx: LogicalContext, mr: ast.MakeResults) = {
    val genPlan = withColumn(ctx,
      withColumn(ctx,
        withColumn(ctx,
          withColumn(ctx,
            withColumn(ctx,
              withColumn(ctx,
                withColumn(ctx,
                  Range(0, mr.count, 1, numSlices = None),
                  ctx.rawFieldName, Literal(null)),
                ctx.timeFieldName, CurrentTimestamp()),
              "host", Literal(null)),
            "source", Literal(null)),
          "sourcetype", Literal(null)),
        "splunk_server", Literal(mr.splunkServer)),
      "splunk_server_group", Literal(mr.splunkServerGroup))

    if (!mr.annotate) {
      Project(Seq(
        UnresolvedAttribute(ctx.timeFieldName)
      ), genPlan)
    } else {
      Project(Seq(
        UnresolvedAttribute(ctx.rawFieldName),
        UnresolvedAttribute(ctx.timeFieldName),
        UnresolvedAttribute("host"),
        UnresolvedAttribute("source"),
        UnresolvedAttribute("sourcetype"),
        UnresolvedAttribute("splunk_server"),
        UnresolvedAttribute("splunk_server_group")
      ), genPlan)
    }
  }

  private def applyMvExpand(ctx: LogicalContext, tree: LogicalPlan, field: ast.Field,
                            limit: Option[Int]) = {
    val attribute = attr(field)
    val explodedAttr = Explode(if (limit.isEmpty) {
        attribute
      } else {
        Slice(attribute, Literal(1), Literal(limit.get))
      })
    withColumn(ctx, tree, field.value, explodedAttr)
  }

  private def applyBin(ctx: LogicalContext, tree: LogicalPlan, bc: ast.BinCommand) = {
    val (field, alias) = bc.field match {
      case ast.Field(v) => (UnresolvedAttribute(v), v)
      case ast.Alias(ast.Field(v), alias) => (UnresolvedAttribute(v), alias)
      case _ => throw new ConversionFailure(s"bin field ${bc.field}")
    }
    if (bc.span.isEmpty) {
      throw new ConversionFailure(s"Currently, only `span` is implemented: $bc")
    }
    val ts = bc.span.get.asInstanceOf[ast.TimeSpan]
    val duration = Literal.create(timeSpan(ts).toString())
    // technically, we can do it in one stage, but generated code would be less readable
    val projectWindow = withColumn(ctx, tree, alias, new TimeWindow(field, duration))
    withColumn(ctx, projectWindow, alias, UnresolvedAttribute(Seq(alias, "start")))
  }

  private def applyAddTotals(ctx: LogicalContext, tree: LogicalPlan, at: ast.AddTotals) = {
    // TOOO implement `col` & `label` & `labelfield` parameters behaviour
    // When no columns are of numeric type this function will return 0.0 whereas it should be null
    if (ctx.output.isEmpty && at.fields.isEmpty) {
      throw EmptyContextOutput(at)
    }

    if (at.row) {
      val fieldsToSum = if (at.fields.isEmpty) ctx.output else at.fields.map(attr)
      withColumn(ctx, tree, at.fieldName, fieldsToSum.map(expr =>
        CaseWhen(Seq((IsNotNull(Cast(expr, DoubleType)), expr)),
          Some(Literal(0.0))).asInstanceOf[Expression]
      ).reduceLeft {
        (toReturnExpr, expr) => Add(expr, toReturnExpr)
      })
    } else tree
  }
}


