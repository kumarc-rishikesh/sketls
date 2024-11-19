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
  "com.crobox.clickhouse" %% "client" % "1.2.6",
  "org.slf4j" % "slf4j-simple" % "1.7.36",
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "6.0.1"

)
resolvers += "Akka library repository".at("https://repo.akka.io/maven")