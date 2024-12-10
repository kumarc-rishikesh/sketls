name := "scetls"

version := "0.1"

organization := "com.neu"

scalaVersion := "2.13.13"

val akkaVersion = "2.8.8"
val akkaHTTPVersion = "10.5.3"

libraryDependencies ++= Seq(
  //akka streams
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
  "com.typesafe.slick" %% "slick" % "3.4.1",
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0",
  "org.postgresql" % "postgresql" % "42.6.0",
  "software.amazon.awssdk" % "s3" % "2.19.33",
  "com.typesafe" % "config" % "1.4.2",
  "com.crobox.clickhouse" %% "client" % "1.2.6",
  "org.slf4j" % "slf4j-simple" % "1.7.36",
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "6.0.1",
  "org.typelevel" %% "cats-core" % "2.9.0",
  "ch.qos.logback" % "logback-classic" % "1.4.11",
  "org.apache.commons" % "commons-csv" % "1.10.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",

  // Test libs
  "org.scalatest" %% "scalatest" % "3.2.16" % Test,
  "org.mockito" %% "mockito-scala" % "1.17.12" % Test
)
resolvers += "Akka library repository".at("https://repo.akka.io/maven")