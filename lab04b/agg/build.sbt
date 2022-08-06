ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "agg"
  )


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.4.5"
)