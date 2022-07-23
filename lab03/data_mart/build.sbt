ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.7"

lazy val root = (project in file("."))
  .settings(
    name := "data_mart"
  )

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.6.2",
  "org.postgresql" % "postgresql" % "42.2.12",
  "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
  "org.apache.spark" %% "spark-core" % "2.4.7"
)