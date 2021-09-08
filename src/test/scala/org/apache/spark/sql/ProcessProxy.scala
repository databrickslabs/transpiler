package org.apache.spark.sql

import scala.collection.mutable
import scala.sys.process.{Process, ProcessLogger}
import org.apache.logging.log4j.scala.Logging


class Capture extends ProcessLogger {
  private val stdout = new mutable.StringBuilder()
  private val stderr = new mutable.StringBuilder()
  override def out(s: => String): Unit = stdout ++= s
  override def err(s: => String): Unit = stderr ++= s
  override def buffer[T](f: => T): T = f
  def getOutput: String = stdout.toString
  def getError: String = stderr.toString
}

trait ProcessProxy extends Logging {
  private val folder = getClass.getResource("/").getFile

  lazy val spark = {
    SparkSession.builder()
      .withExtensions(e => new TermExtension().apply(e))
      .master("local[1]")
      .config("spark.sql.warehouse.dir", s"$folder/warehouse")
      .getOrCreate()
  }

  def launchPy = {
    val pb = Process(s"$folder/t.py",
      new java.io.File(folder),
      "SPARK_MASTER" -> s"spark://localhost:${spark.conf.get("spark.driver.port")}",
      "PATH" -> "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
    val pl = new Capture
    pb.!(pl)

    print(pl.getOutput)
  }

  def generates(search: String, code: String) =
    assert(Transpiler.toPython(search) == code)

  def executes(search: String, results: String, truncate: Int = 0) = {
    val testVar = Transpiler.toDataFrame(spark, search)
                            .showString(20, truncate)
    logger.debug(s"Test:\n${testVar}")
    logger.debug(s"Expected:\n${results}")
    assert(testVar == results)
  }

}
