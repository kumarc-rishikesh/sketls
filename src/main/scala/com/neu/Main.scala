package com.neu

import com.neu.Pipeline.Generator.PipelineGenerator
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{Materializer, SystemMaterializer}
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext

object Main extends App {
  println("Hello world")
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("LocalStack S3 Example")
    .config("spark.master", "local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")              // LocalStack S3 endpoint
    .config("spark.hadoop.fs.s3a.access.key", "test")                             // Dummy credentials for LocalStack
    .config("spark.hadoop.fs.s3a.secret.key", "test")                             // Dummy credentials for LocalStack
    .config("spark.hadoop.fs.s3a.path.style.access", "true")                      // Required for LocalStack
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") // Set the implementation for s3a
    .config(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )                                                                             // Use Simple Credentials Provider
    .getOrCreate()

  implicit val actorSystem: ActorSystem   = ActorSystem("scetls")
  implicit val ec: ExecutionContext       = actorSystem.dispatcher
  implicit val materializer: Materializer = SystemMaterializer(actorSystem).materializer

  val configSource = scala.io.Source.fromFile("./docs/sample_config.yaml")
  val yamlString   = configSource.mkString
  configSource.close()

  val generator = new PipelineGenerator(spark, ec, materializer, actorSystem)
  val jobMap    = generator.generateJobFunctions(yamlString)

  jobMap match {
    case Right(x) =>
      x.foreach(fn_time => fn_time._1())
  }
}
