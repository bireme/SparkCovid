ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10" //"2.12.17"

//ThisBuild / useCoursier := false  // Para poder compilar com scala 2.13.10

lazy val root = (project in file("."))
  .settings(
    name := "SparkCovid"
  )

lazy val intellijRunner = project.in(file("intellijRunner")).dependsOn(RootProject(file("."))).settings(
  scalaVersion := "2.13.10",
  libraryDependencies ++= sparkDependencies.map(_ % "compile")
).disablePlugins(sbtassembly.AssemblyPlugin)

val sparkVersion = "3.4.0" //"3.3.1"
val mongoSparkConnectorVersion = "10.1.1" //"10.0.2"
val sparkExcelVersion = "3.3.1_0.18.5"
val jodaTimeVersion = "2.12.4"

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion, /*% "provided",*/
  "org.mongodb.spark" %% "mongo-spark-connector" % mongoSparkConnectorVersion,
  "com.crealytics" %% "spark-excel" % sparkExcelVersion,
  "joda-time" % "joda-time" % jodaTimeVersion
)

libraryDependencies ++= sparkDependencies.map(_ % "provided")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Ywarn-unused")

/*
assembly / assemblyMergeStrategy := {
  case "module-info.class" => MergeStrategy.first //MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
*/

/*
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
*/

assembly / assemblyMergeStrategy  := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

