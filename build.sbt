name := "SparkTask1"
version := "0.1"
scalaVersion := "2.11.8"
mainClass in Compile := Some("Task1")
assemblyJarName in assembly := "sparkStreaming.jar"

val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
//  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0",
  "org.apache.parquet" % "parquet-avro" % "1.8.1"
)


scalaBinaryVersion in ThisBuild := "2.11"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                           => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.12" % Test,
  "org.scalatest" % "scalatest_2.11" % "3.0.0" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "log4j" % "log4j" % "1.2.17",
  "ch.hsr" % "geohash" % "1.3.0",
  "org.apache.parquet" % "parquet-avro" % "1.8.1"
)