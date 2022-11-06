# Databricks notebook source
def transpile(spl: str):
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

#spark.read.csv('dbfs:/databricks-datasets/COVID/coronavirusdataset/Weather.csv', header=True).createOrReplaceTempView('weather')
# COMMAND ----------
transpile("index=fake | bin span=5m timestamp | stats count by timestamp | sort timestamp")
# COMMAND ----------
transpile("index=fake | stats count(id) by gender")
# COMMAND ----------
transpile("index=fake | eval len_ip=len(ipAddress), len_mail=len(email) | fields +gender, len_ip, len_mail | stats sum(len_*) by gender | sort gender")
# COMMAND ----------
transpile("index=fake | eval len_ip=len(ipAddress), len_mail=len(email) | stats min(len_*) AS min_* by gender | sort gender")
# COMMAND ----------
transpile("index=fake | eval len_mail=len(email) | fields +id, gender, cardNumber, len_mail | stats max(*) AS max_* by gender | sort gender")
# COMMAND ----------
transpile("index=fake | id > 17 | fields + id, gender, email, ipAddress")
# COMMAND ----------
transpile("index=fake | eval array_count=mvcount(array) | where array_count=1 | fields + id, gender, email, array, array_count")
# COMMAND ----------
transpile("index=fake | eval test = mvfilter(len(array) = 2) | fields + array, test")
# COMMAND ----------
transpile("index=fake | eval test = mvcount(mvfilter(len(array) = 2)) | eval original_count = mvcount(array) | where tonumber(original_count) > tonumber(test) | fields + array, original_count, test")
# COMMAND ----------
transpile("index=fake | eval email_len = len(email) | id > len(email) - 10 | fields + id, email, email_len")
# COMMAND ----------
transpile("index=fake | eval test = substr(country, 3) | fields country, test")
# COMMAND ----------
transpile("index=fake | eval test = substr(country, 3, 3) | fields country, test")
# COMMAND ----------
transpile("index=fake | eval test = substr(country, -5) | fields country, test")
# COMMAND ----------
transpile("index=fake | rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields +_raw, from, to")
# COMMAND ----------
transpile("index=fake | rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields +from, to")
# COMMAND ----------
transpile("index=fake | fields +id, gender, country, email | rename country as pays")
# COMMAND ----------
transpile("index=fake | rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields +from, to | rename from AS emailFrom, to AS emailTo")
# COMMAND ----------
transpile("index=fake | rex \"From: <(?<from>.*)> To: <(?<to>.*)>\" | fields - _raw | return 4 emailFrom=from emailTo=to")
# COMMAND ----------
transpile("index=fake | join type=inner id [search index=fake_for_join] | fields +id, email, sport")
# COMMAND ----------
transpile("index=fake | join type=left id [search index=fake_for_join] | fields +id, email, sport")
# COMMAND ----------
transpile("multisearch [index=fake | id < 2 | fields +id, gender] [index=fake_for_join | id < 2] [index=fake | id < 2] | fields +id, gender, sport")
# COMMAND ----------
transpile("index=fake | fields +email | eval b_not_null=if(isnotnull(email), 1, 0)")
# COMMAND ----------
transpile("index=fake | eval id_null=if(id > 10, null(), id) | table id id_null")
# COMMAND ----------
transpile("index=fake_with_double | fields +id, score | fillnull")
# COMMAND ----------
transpile("index=fake | fields +id, email | fillnull")
# COMMAND ----------
transpile("index=fake | fields +id, email | fillnull value=NA")
# COMMAND ----------
transpile("index=fake | fields +id, email, gender, ipAddress | fillnull value=NA email gender")
# COMMAND ----------
transpile("index=fake | eval n = len(email) | fields +id, email, gender, n | eventstats max(n) AS max_n, min(n) by gender")
# COMMAND ----------
transpile("index=fake | eval _time=timestamp | streamstats count(_time) AS n | eval cat=if(n < 4, \"A\",\"B\") | table _time cat n | streamstats max(n) AS max_n, min(n) by cat | streamstats current=false window=2 min(n) AS min_n_lag")
# COMMAND ----------
transpile("index=fake | id > 10 | eval min=min(id,15), max=max(id,15) | fields + id, min, max")
# COMMAND ----------
transpile("index=fake | eval static=10 | fields + gender, static | dedup 1 gender static")
# COMMAND ----------
transpile("inputlookup fake where id < 3 | fields +id, gender, email, ipAddress, country")
# COMMAND ----------
transpile("inputlookup max=2 fake where id > 10 | fields +id, gender, email, ipAddress, country")
# COMMAND ----------
transpile("index=fake | fields +id, gender, email | format maxresults=2")
# COMMAND ----------
transpile("index=fake | fields +id, gender, email | format maxresults=2 \"[\" \"[\" \"&&\" \"]\" \"||\" \"]\"")
# COMMAND ----------
transpile("index=fake | fields +id, country | mvcombine id")
# COMMAND ----------
transpile("index=fake | fields +id, country | mvcombine delim=\";\" id")
# COMMAND ----------
transpile("index=fake | fields +id, array | mvexpand array | where id=1")
# COMMAND ----------
transpile("index=fake | fields +id, array | mvexpand array limit=1 | where id=1")
# COMMAND ----------
transpile("makeresults count=5 annotate=t server_group=\"group1\" | fields - _time")
# COMMAND ----------
transpile("index=fake | id < 5 | eval in_range = if(cidrmatch(\"109.177.0.0/16\", ipAddress),1,0) | fields +id, ipAddress, in_range")
# COMMAND ----------
transpile("index=fake | id < 5 | ipAddress=109.177.0.0/16 | fields +id, ipAddress")
# COMMAND ----------
transpile("index=fake | id < 5 | eval quant=round((id/3),2), unit=if(id < 3, \"M\", \"G\") | eval size=quant.unit, memk=memk(size) | fields +id, size, memk")
# COMMAND ----------
transpile("index=fake | id < 5 | eval unit=if(id < 3, \"Megabyte\", \"GB\") | eval size=id.unit, rmunit=rmunit(size) | fields +id, size, rmunit")
# COMMAND ----------
transpile("index=fake | id < 5 | eval s=substr(ipAddress,1,3).\",\".substr(ipAddress, 5, 2) | eval n=rmcomma(s) | fields +id, s, n")
# COMMAND ----------
transpile("index=fake | id < 3 | convert timeformat=\"%H\" ctime(timestamp) AS hour | fields +id, timestamp, hour")
# COMMAND ----------
transpile("index=fake | id < 3 | convert ctime(timestamp) AS ctime | fields +id, timestamp, ctime")
# COMMAND ----------
transpile("index=fake | id < 3 | fields +id, cardType, cardNumber | convert num(card*) none(cardType)")
# COMMAND ----------
transpile("index=fake | id < 3 | fields +id, ipAddress, cardType, cardNumber | convert auto(*) none(i*)")
# COMMAND ----------
transpile("index=fake | id < 3 | eval date = strftime(timeStamp, \"%m/%d/%Y %H:%M:%S\") | eval hour=strftime(\"2021-11-05 21:20:32\", \"%H\") | fields +id, date, hour")
# COMMAND ----------
transpile("index=fake | eval anotherNum=10 | fields +id, gender, anotherNum | addtotals fieldname=my_total")
# COMMAND ----------
transpile("index=fake | map search=\"search index=fake_for_join id=$id$\"")
