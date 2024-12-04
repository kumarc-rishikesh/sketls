name := "scetls"

version := "0.1"

organization := "com.neu"

scalaVersion := "2.13.14"

val akkaVersion = "2.8.8"
val akkaHTTPVersion = "10.5.3"

libraryDependencies ++= Seq(
  //akka streams
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
  "com.typesafe.slick" %% "slick" % "3.5.2",
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0", // Or latest version
  "org.postgresql" % "postgresql" % "42.6.0" // Use the latest version
)
