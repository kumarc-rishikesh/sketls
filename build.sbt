name := "scetls"
version := "0.1"
organization := "com.neu"
scalaVersion := "2.13.13"

val akkaVersion = "2.8.8"
val akkaHttpVersion = "10.5.3"
val sparkVersion = "3.4.0"

dependencyOverrides += "org.scala-lang" % "scala-library" % "2.13.13"

libraryDependencies ++= Seq(
  // Akka
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % "2.6.20",

  // Circe
  "io.circe" %% "circe-yaml" % "1.15.0",
  "io.circe" %% "circe-generic-extras" % "0.14.4",

  // Cats
  "org.typelevel" %% "cats-core" % "2.12.0",

  // Scala
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,

  // Spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // AWS
  "org.apache.hadoop" % "hadoop-aws" % "3.2.0",
  "software.amazon.awssdk" % "s3" % "2.20.49",

  // Database
  "com.crobox.clickhouse" %% "client" % "1.2.6",
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "6.0.1",

  // Configuration & Logging
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0",
  "org.slf4j" % "slf4j-simple" % "1.7.36",
  "ch.qos.logback" % "logback-classic" % "1.4.12"
)

resolvers += "Akka library repository".at("https://repo.akka.io/maven")