ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "SparkCovid"
  )

/* Descomente (intellijRunner) para executar com Intellij e utilizar Debug */

//lazy val intellijRunner = project.in(file("intellijRunner")).dependsOn(RootProject(file("."))).settings(
//  scalaVersion := "2.13.10",
//).disablePlugins(sbtassembly.AssemblyPlugin)

val sparkVersion = "3.4.0" //"3.3.1"
val mongoSparkConnectorVersion = "10.1.1" //"10.0.2"
val sparkExcelVersion = "3.3.1_0.18.5"
val jodaTimeVersion = "2.12.5"
val log4j = "1.2.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion, /*% "provided",*/
  "org.mongodb.spark" %% "mongo-spark-connector" % mongoSparkConnectorVersion,
  "com.crealytics" %% "spark-excel" % sparkExcelVersion,
  "joda-time" % "joda-time" % jodaTimeVersion,
  "log4j" % "log4j" % log4j,
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Ywarn-unused")

assembly / assemblyMergeStrategy  := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}