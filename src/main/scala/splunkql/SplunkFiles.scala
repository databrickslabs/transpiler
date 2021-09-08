package splunkql

import java.io.{FileInputStream, InputStream}

import scala.collection.JavaConverters._
import fastparse.{Parsed, parse}
import org.apache.spark.sql.{PythonGenerator, SplToCatalyst}
import org.ini4j.Ini

case class SavedSearch(name: String, search: String, cron: String)

class SearchesFile(is: InputStream) {
  private val searchesConf = new Ini()
  searchesConf.load(is)

  private val searches = searchesConf.keySet().asScala.map(name => {
    val config = searchesConf.get(name)
    SavedSearch(name, config.get("search"), config.get("cron_schedule"))
  }).filter(_.search != null).map(search => search.name -> search).toMap

  def get(name: String) = searches.get(name)
}

case class Macro(name: String, definition: String, iseval: Boolean)

class MacrosFile(is: InputStream) {
  private val macrosConf = new Ini()
  macrosConf.load(is)

  private val macros = macrosConf.keySet().asScala map { name =>
    Macro(name, macrosConf.get(name, "definition"),
      macrosConf.get(name, "iseval") != "0")
  }

  def expand(raw: String): String = macros.foldLeft(raw)((t, m) =>
    t.replace(s"`${m.name}`", m.definition))
}

case class SplunkContext(sf: SearchesFile, mf: MacrosFile) {
  def getSearch(name: String) =
    sf.get(name).map(savedSearch =>
      mf.expand(savedSearch.search))

  def parseSearch(name: String) =
    getSearch(name).map { search =>
      parse(search, spl.SplParser.pipeline(_), verboseFailures = true) match {
        case Parsed.Success(value, _) => value
        case f: Parsed.Failure =>
          throw new AssertionError(f.trace().longMsg)
    }}

  def getSearchInCatalyst(name: String) = {
    val tsc = new SplToCatalyst()
    parseSearch(name).map(tsc.process)
  }

  def generatePython(name: String) = {
    val pg = new PythonGenerator()
    getSearchInCatalyst(name).map(pg.fromPlan)
  }
}