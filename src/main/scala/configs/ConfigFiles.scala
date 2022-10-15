package configs

import java.io.InputStream

import org.ini4j.Ini
import spl.Transpiler

import scala.collection.JavaConverters._

case class SavedSearch(name: String, search: String, cron: String)

class SearchesFile(is: InputStream) {
  private val searchesConf = new Ini()
  searchesConf.load(is)

  private val searches = searchesConf.keySet().asScala.map(name => {
    val config = searchesConf.get(name)
    SavedSearch(name, config.get("search"), config.get("cron_schedule"))
  }).filter(_.search != null).map(search => search.name -> search).toMap

  def get(name: String): Option[SavedSearch] = searches.get(name)
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

case class ConfigContext(sf: SearchesFile, mf: MacrosFile) {
  def getSearch(name: String): Option[String] =
    sf.get(name).map(savedSearch =>
      mf.expand(savedSearch.search))

  def generatePython(name: String): Option[String] = {
    getSearch(name).map(Transpiler.toPython)
  }
}
