name := "spark-splunkql"

version := "0.1"

scalaVersion := "2.12.12"
//scalacOptions ++= Seq("-feature")
val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "fastparse" % "2.2.2",
  "org.ini4j" % "ini4j" % "0.5.4",

  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.13.0" % Runtime,

  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
)

//// ScalaTest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"
