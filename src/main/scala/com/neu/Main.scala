package com.neu

import akka.actor.ActorSystem
import com.neu.connectors.{S3Connector, S3LocalstackActions}
import scala.concurrent.ExecutionContext
import org.apache.spark.sql.SparkSession

object Main extends App {
  implicit val system: ActorSystem = ActorSystem("localstack-s3-example")
  implicit val ec: ExecutionContext = system.dispatcher

  // Initialize SparkSession
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("LocalStack S3 Example")
    .config("spark.master", "local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")    // LocalStack S3 endpoint
    .config("spark.hadoop.fs.s3a.access.key", "test")                   // Dummy credentials for LocalStack
    .config("spark.hadoop.fs.s3a.secret.key", "test")                   // Dummy credentials for LocalStack
    .config("spark.hadoop.fs.s3a.path.style.access", "true")            // Required for LocalStack
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  // Set the implementation for s3a
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")  // Use Simple Credentials Provider
    .config("hadoop.home.dir", "C:\\Users\\mitha\\Downloads\\winutils\\hadoop-3.0.0\\bin")
    .getOrCreate()

  val s3Connector = S3Connector()

  S3LocalstackActions.performS3Operations()(system, ec, s3Connector, spark)
    .onComplete(_ => {
      spark.stop()
      system.terminate()
    })
}