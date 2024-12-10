name := "scetls"

version := "0.1"

organization := "com.neu"

scalaVersion := "2.13.13"

dependencyOverrides += "org.scala-lang" % "scala-library" % "2.13.13"

val akkaVersion = "2.8.8"
val akkaHTTPVersion = "10.5.3"

libraryDependencies ++= Seq(
  //akka streams
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
  "io.circe" %% "circe-yaml" % "1.15.0",
  "io.circe" %% "circe-generic-extras" % "0.14.4",
  "org.typelevel" %% "cats-core" % "2.12.0",
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)
resolvers += "Akka library repository".at("https://repo.akka.io/maven")