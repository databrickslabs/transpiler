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
        IntValue(12)
      ),
      Or,
      Binary(
        Binary(
          Value("event_id"),
          Equals,
          IntValue(13)
        ),
        Or,
        Binary(
          Value("event_id"),
          Equals,
          IntValue(14)
        )
      )
    ))
  }

  test("a=b b=c (c=f OR d=t)") {
    p(impliedSearch(_), Binary(
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
          Bool(false)
        ),
        Or,
        Binary(
          Value("d"),
          Equals,
          Bool(true)
        )
      )
    ))
  }

  test("code IN(4*, 5*)") {
    p(impliedSearch(_), SearchCommand(
      FieldIn("code", Seq(
        Value("4*"),
        Value("5*")
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
        Map(),
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
}
