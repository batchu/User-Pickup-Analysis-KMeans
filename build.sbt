name := "DataAnalytics"

version := "1.0"

scalaVersion := "2.12.1"


name := "SparkSimpleTest"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-streaming" % "2.2.0",
  "org.apache.spark" %% "spark-hive" % "2.2.0",
  "org.apache.spark" %% "spark-graphx" % "2.2.0",
  "org.apache.spark" %% "spark-mllib" % "2.2.0"
)