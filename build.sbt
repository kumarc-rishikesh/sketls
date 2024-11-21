name := "scetls"

version := "0.1"

organization := "com.neu"

scalaVersion := "2.13.12"

val akkaVersion = "2.8.8"
val akkaHTTPVersion = "10.5.3"

libraryDependencies ++= Seq(
  //akka streams
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
  "software.amazon.awssdk" % "s3" % "2.19.33",
  "com.typesafe" % "config" % "1.4.2"
)
