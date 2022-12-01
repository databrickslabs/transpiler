def transpile(query: str):
    """Transpiles a query into PySpark DataFrame DSL"""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    spl = spark._jvm.com.databricks.labs.transpiler.spl
    code = spl.Transpiler.toPython(query)
    print(code)

def spl(query: str, do_display=False) -> 'pyspark.sql.DataFrame':
    from pyspark.sql import SparkSession, DataFrame
    spark = SparkSession.getActiveSession()
    spl = spark._jvm.com.databricks.labs.transpiler.spl
    jdf = spl.Transpiler.toDataFrame(spark._jsparkSession, query)
    df = DataFrame(jdf, spark)
    if not do_display:
        return df
    else:
        global display
        display(df)