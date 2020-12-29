name := "spark-scd"

version := "0.1"

scalaVersion := "2.11.8"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies +="org.apache.spark" %% "spark-hive" % "2.4.4"


//configuration file dependency
libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "com.typesafe" % "config" % "1.2.1")

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.15"