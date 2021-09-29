package spl

class SplParserTest extends ParserSuite {
  import spl.SplParser._

  test("f") {
    p(bool(_), Bool(false))
  }

  test("false") {
    p(bool(_), Bool(false))
  }

  test("t") {
    p(bool(_), Bool(true))
  }

  test("true") {
    p(bool(_), Bool(true))
  }

  test("right") {
    p(fvalue(_), Value("right"))
  }

  test("left") {
    p(field(_), Value("left"))
  }

  test("foo   = bar") {
    p(fieldAndValue(_), FV("foo", "bar"))
  }

  test("foo=bar bar=baz") {
    p(fieldAndValueList(_), Map(
      "foo" -> "bar",
      "bar" -> "baz"
    ))
  }

  test("a ,   b,c, d") {
    p(fieldList(_), Seq(
      Value("a"),
      Value("b"),
      Value("c"),
      Value("d"),
    ))
  }

  test("D:\\Work\\Stuff.xls") {
    p(filename(_), "D:\\Work\\Stuff.xls")
  }

  test("-100500") {
    p(int(_), IntValue(-100500))
  }

  test("a OR b") {
    p(expr(_), Binary(
      Value("a"),
      Or,
      Value("b")
    ))
  }

  // TODO: add wildcard AST transformation
  test("productID=\"S*G01\"") {
    p(expr(_), Binary(
      Value("productID"),
      Equals,
      Value("S*G01")
    ))
  }

  test("(event_id=12 OR event_id=13 OR event_id=14)") {
    p(expr(_), Binary(
      Binary(
        Value("event_id"),
        Equals,
        Value("12")
      ),
      Or,
      Binary(
        Binary(
          Value("event_id"),
          Equals,
          Value("13")
        ),
        Or,
        Binary(
          Value("event_id"),
          Equals,
          Value("14")
        )
      )
    ))
  }

  test("a=b b=c (c=f OR d=t)") {
    p(impliedSearch(_), SearchCommand(Binary(
      Binary(
        Binary(
          Value("a"),
          Equals,
          Value("b")
        ),
        And,
        Binary(
          Value("b"),
          Equals,
          Value("c")
        )
      ),
      And,
      Binary(
        Binary(
          Value("c"),
          Equals,
          Value("f")
        ),
        Or,
        Binary(
          Value("d"),
          Equals,
          Value("t")
        )
      )
    )))
  }

  test("code IN(4*, 5*)") {
    p(impliedSearch(_), SearchCommand(
      FieldIn("code", Seq(
        Value("4*"),
        Value("5*")
      ))))
  }

  test("var_5 IN (str_2 str_3)") {
    p(impliedSearch(_), SearchCommand(
      FieldIn("var_5", Seq(
        Value("str_2"),
        Value("str_3"),
      ))))
  }

  test("NOT code IN(4*, 5*)") {
    p(impliedSearch(_), SearchCommand(
      Unary(UnaryNot,
        FieldIn("code", Seq(
          Value("4*"),
          Value("5*"))))
    ))
  }

  test("code IN(10, 29, 43) host!=\"localhost\" xqp>5") {
    p(impliedSearch(_), SearchCommand(
      Binary(
        Binary(
          FieldIn("code", Seq(Value("10"), Value("29"), Value("43"))),
          And,
          Binary(
            Value("host"),
            NotEquals,
            Value("localhost")
          )
        ),
        And,
        Binary(
          Value("xqp"),
          GreaterThan,
          Value("5")
        )
      )
    ))
  }

  test("head 20") {
    p(head(_),
      HeadCommand(
        IntValue(20),
        Bool(false),
        Bool(false)
      )
    )
  }

  test("head limit=400") {
    p(head(_),
      HeadCommand(
        IntValue(400),
        Bool(false),
        Bool(false))
    )
  }

  test("head limit=400 keeplast=true null=false") {
    p(head(_),
      HeadCommand(
        IntValue(400),
        Bool(true),
        Bool(false)
      )
    )
  }

  test("head count>10") {
    p(head(_),
      HeadCommand(
        Binary(
          Value("count"),
          GreaterThan,
          Value("10")
        ),
        Bool(false),
        Bool(false)
      )
    )
  }

  test("fields column_a, column_b, column_c") {
    p(fields(_),
      FieldsCommand(
        None,
        Seq(
          Value("column_a"),
          Value("column_b"),
          Value("column_c")
        )
      )
    )
  }

  test("fields + column_a, column_b") {
    p(fields(_),
      FieldsCommand(
        Option("+"),
        Seq(
          Value("column_a"),
          Value("column_b")
        )
      )
    )
  }

  test("fields - column_a, column_b") {
    p(fields(_),
      FieldsCommand(
        Option("-"),
        Seq(
          Value("column_a"),
          Value("column_b")
        )
      )
    )
  }

  test("sort A, -B, +num(C)") {
    p(sort(_),
      SortCommand(
        Seq(
          (None, Value("A")),
          (Some("-"), Value("B")),
          (Some("+"), Call("num", Seq(Value("C"))))
        )
      )
    )
  }

  test("TERM(XXXXX*\\\\XXXXX*)") {
    p(pipeline(_), Pipeline(Seq(
      SearchCommand(
        Call("TERM",List(Value("XXXXX*\\\\XXXXX*")))
      )
    )))
  }

  test("values(eval(mvappend(\"a: \" . a, \"b: \" . b)))") {
    p(pipeline(_), Pipeline(Seq(
      SearchCommand(
        Call("values", Seq(
          Call("eval", Seq(
            Call("mvappend",
              Seq(
                Binary(
                  Value("a: "),
                  Concatenate,
                  Value("a")
                ),
                Binary(
                  Value("b: "),
                  Concatenate,
                  Value("b")
                )))))))))))
  }

  test("sort A") {
    p(sort(_),
      SortCommand(
        Seq(
          (None, Value("A")),
        )
      )
    )
  }

  test("eval mitre_category=\"Discovery\"") {
    p(eval(_), EvalCommand(Seq(
      (Value("mitre_category"), Value("Discovery"))
    )))
  }

  test("eval hash_sha256= lower(hash_sha256), b=c") {
    p(eval(_), EvalCommand(Seq(
      (Value("hash_sha256"), Call("lower", Seq(Value("hash_sha256")))),
      (Value("b"), Value("c"))
    )))
  }

  test("convert ctime(indextime)") {
    p(convert(_), ConvertCommand(None, Seq(
      FieldConversion("ctime", Value("indextime"), None)
    )))
  }

  test("collect index=threathunting a=b x, y,  z") {
    p(pipeline(_), Pipeline(Seq(
      CollectCommand(Map(
        "index" -> "threathunting",
        "a" -> "b"
      ), Seq(
        Value("x"),
        Value("y"),
        Value("z"),
      ))
    )))
  }

  test("index=foo bar=baz | eval foo=bar | collect index=newer") {
    p(pipeline(_), Pipeline(Seq(
      SearchCommand(
        Binary(
          Binary(
            Value("index"),
            Equals,
            Value("foo")
          ),
          And,
          Binary(
            Value("bar"),
            Equals,
            Value("baz")
          )
        )
      ),
      EvalCommand(Seq(
        (Value("foo"),Value("bar"))
      )),
      CollectCommand(Map(
        "index" -> "newer"
      ),Seq())
    )))
  }

  test("lookup process_create_whitelist a b output reason") {
    p(pipeline(_), Pipeline(Seq(
      LookupCommand(
        "process_create_whitelist",
        Seq(
          Value("a"),
          Value("b")
        ),
        Some(
          LookupOutput(
            "output",
            Seq(
              Value("reason")
            )
          )
        )
      )
    )))
  }

  test("where isnull(reason)") {
    p(pipeline(_), Pipeline(Seq(
      WhereCommand(
        Call(
          "isnull",Seq(
            Value("reason")
          )
        )
      )
    )))
  }

  test("table foo bar baz*") {
    p(pipeline(_), Pipeline(Seq(
      TableCommand(Seq(
        Value("foo"),
        Value("bar"),
        Value("baz*"),
      ))
    )))
  }

  test("stats first(startTime) AS startTime, last(histID) AS lastPassHistId BY testCaseId") {
    p(pipeline(_), Pipeline(Seq(
      StatsCommand(Map(),Seq(
        Alias(
          Call("first",Seq(
            Value("startTime")
          )),
          "startTime"),
        Alias(
          Call("last",Seq(
            Value("histID")
          )),
          "lastPassHistId")
      ),
      Seq(
        Value("testCaseId")
      ))
    )))
  }

  test("stats count(eval(status=404))") {
    p(pipeline(_), Pipeline(Seq(
      StatsCommand(Map(),Seq(
        Call("count",Seq(
          Call("eval",Seq(
            Binary(
              Value("status"),
              Equals,
              Value("404")
            ))))))
      ))
    ))
  }

  test("no-comma stats") {
    val query =
      """stats allnum=f delim=":" partitions=10 count earliest(_time) as earliest latest(_time) as latest
        |values(var_2) as var_2
        |by var_1
        |""".stripMargin
    parses(query, stats(_), StatsCommand(
      Map(
        "allnum" -> "f",
        "delim" -> ":",
        "partitions" -> "10"
      ),
      Seq(
        Call("count"),
        Alias(Call("earliest", Seq(Value("_time"))), "earliest"),
        Alias(Call("latest", Seq(Value("_time"))), "latest"),
        Alias(Call("values", Seq(Value("var_2"))), "var_2"),
      ),
      Seq(
        Value("var_1")
      )
    ))
  }

  test("rex field=savedsearch_id max_match=10 \"(?<user>\\w+);(?<app>\\w+);(?<SavedSearchName>\\w+)\"") {
    p(pipeline(_), Pipeline(Seq(
      RexCommand(
        Some("savedsearch_id"),
        10,
        None,
        None,
        "(?<user>\\w+);(?<app>\\w+);(?<SavedSearchName>\\w+)"
      )
    )))
  }

  test("rex mode=sed \"s/(\\d{4}-){3}/XXXX-XXXX-XXXX-/g\"") {
    p(pipeline(_), Pipeline(Seq(
      RexCommand(
        None,
        1,
        None,
        Some("sed"),
        "s/(\\d{4}-){3}/XXXX-XXXX-XXXX-/g"
      )
    )))
  }

  test("rename _ip AS IPAddress") {
    p(rename(_),
      RenameCommand(
        Seq(Alias(
          Value("_ip"),
          "IPAddress"
        )))
    )
  }

  test("rename _ip AS IPAddress, _host AS host, _port AS port") {
    p(rename(_),
      RenameCommand(Seq(
        Alias(
          Value("_ip"),
          "IPAddress"
        ),
        Alias(
          Value("_host"),
          "host"
        ),
        Alias(
          Value("_port"),
          "port"
        )))
    )
  }

  // Regex not taken into account
  test("rename foo* AS bar*") {
    p(rename(_),
      RenameCommand(
        Seq(Alias(
          Value("foo*"),
          "bar*"
        )))
    )
  }

  test("rename count AS \"Count of Events\"") {
    p(rename(_),
      RenameCommand(
        Seq(Alias(
          Value("count"),
          "Count of Events"
        )))
    )
  }

  test("join product_id [search vendors]") {
    p(join(_),
      JoinCommand(
        joinType = "inner",
        useTime = false,
        earlier = true,
        overwrite = false,
        max = 1,
        Seq(Value("product_id")),
        Pipeline(Seq(
          SearchCommand(Value("vendors"))))
      )
    )
  }

  test("join type=left usetime=true earlier=false overwrite=false product_id, host, name [search vendors]") {
    p(join(_),
      JoinCommand(
        joinType = "left",
        useTime = true,
        earlier = false,
        overwrite = false,
        max = 1,
        Seq(
          Value("product_id"),
          Value("host"),
          Value("name")
        ),
        Pipeline(Seq(
          SearchCommand(Value("vendors"))))
      )
    )
  }

  test("join product_id [search vendors | rename pid AS product_id]") {
    p(join(_),
      JoinCommand(
        joinType = "inner",
        useTime = false,
        earlier = true,
        overwrite = false,
        max = 1,
        Seq(Value("product_id")),
        Pipeline(Seq(
          SearchCommand(Value("vendors")),
          RenameCommand(Seq(
            Alias(
              Value("pid"),
              "product_id"
            )))
        ))
      )
    )
  }

  test("regex _raw=\"(?<!\\d)10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(?!\\d)\"") {
    p(_regex(_), RegexCommand(
      Some((Value("_raw"), "=")),
      "(?<!\\d)10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(?!\\d)"))
  }

  test("regex _raw!=\"(?<!\\d)10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(?!\\d)\"") {
    p(_regex(_), RegexCommand(
      Some((Value("_raw"), "!=")),
      "(?<!\\d)10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(?!\\d)"))
  }

  test("regex \"(?<!\\d)10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(?!\\d)\"") {
    p(_regex(_), RegexCommand(
      None,
      "(?<!\\d)10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(?!\\d)"))
  }

  test("return 10 $test $env") {
    p(_return(_), ReturnCommand(
      Some(IntValue(10)),
      Seq(
        Value("test"),
        Value("env")
      )
    ))
  }

  test("return 10 ip src host port") {
    p(_return(_), ReturnCommand(
      Some(IntValue(10)),
      Seq(
        Value("ip"),
        Value("src"),
        Value("host"),
        Value("port")
      )
    ))
  }

  test("return 10 ip=src host=port") {
    p(_return(_), ReturnCommand(
      Some(IntValue(10)),
      Seq(
        (Value("ip"), Value("src")),
        (Value("host"), Value("port"))
      )
    ))
  }

  test("fillnull") {
    p(fillNull(_), FillNullCommand(None, None))
  }

  test("fillnull value=NA") {
    p(fillNull(_), FillNullCommand(Some("NA"), None))
  }

  test("fillnull value=\"NULL\" host port") {
    p(fillNull(_), FillNullCommand(
      Some("NULL"),
      Some(Seq(
        Value("host"),
        Value("port")
      ))))
  }
}
