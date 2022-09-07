ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "SPark"
  )
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
libraryDependencies += "log4j" % "log4j" % "1.2.14"