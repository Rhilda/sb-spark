ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "features"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
