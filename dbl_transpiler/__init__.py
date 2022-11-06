def transpile(spl: str):
    """Transpiles a query into PySpark DataFrame DSL"""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    code = spark._jvm.spl.Transpiler.toPython(spl)
    print(code)

def spl(query: str, do_display=False) -> 'pyspark.sql.DataFrame':
    from pyspark.sql import SparkSession, DataFrame
    spark = SparkSession.getActiveSession()
    jdf = spark._jvm.spl.Transpiler.toDataFrame(spark._jsparkSession, query)
    df = DataFrame(jdf, spark)
    if not do_display:
        return df
    else:
        global display
        display(df)