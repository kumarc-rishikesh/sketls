package com.neu

import com.neu.connectors.CKHConnector
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{Materializer, SystemMaterializer}
import connectors.CKHActions
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world")
  }
}

//object Main extends App {
//  implicit val system: ActorSystem = ActorSystem("localstack-s3-example")
//  implicit val ec: ExecutionContext = system.dispatcher
//
//  // Initialize SparkSession
//  implicit val spark: SparkSession = SparkSession.builder()
//    .appName("LocalStack S3 Example")
//    .config("spark.master", "local[*]")
//    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")    // LocalStack S3 endpoint
//    .config("spark.hadoop.fs.s3a.access.key", "test")                   // Dummy credentials for LocalStack
//    .config("spark.hadoop.fs.s3a.secret.key", "test")                   // Dummy credentials for LocalStack
//    .config("spark.hadoop.fs.s3a.path.style.access", "true")            // Required for LocalStack
//    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  // Set the implementation for s3a
//    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")  // Use Simple Credentials Provider
//    .getOrCreate()
//
//  val s3Connector = S3Connector()
//
//  S3LocalstackActions.performS3Operations()(system, ec, s3Connector, spark)
//    .onComplete(_ => {
//      spark.stop()
//      system.terminate()
//    })
//}
//
//object Main extends App {
//  implicit val actorSystem: ActorSystem = ActorSystem("clickhouse-example")
//  implicit val ec: ExecutionContext = actorSystem.dispatcher
//  implicit val materializer: Materializer = SystemMaterializer(actorSystem).materializer
//
//  val spark = SparkSession.builder()
//    .appName("scetls")
//    .master("local[*]")
//    .getOrCreate()
//
//  val schema = CrimeData.schema
//  val ckhConnector = CKHConnector()
//  val actions = CKHActions(spark, actorSystem, ckhConnector)
//
//  try {
//    // Chain operations using flatMap
//    val result = for {
//      _ <- Future(println("Reading data into dataferame..."))
//      sourceDF <- actions.readDataCKH("SELECT * FROM crime_data FORMAT CSV", schema)
//
//      _ <- Future(println(s"Read ${sourceDF.count()} rows from crime_data"))
//
//      _ <- Future(println("Writing data to datastore..."))
//      writeResult <- actions.writeDataCKH(sourceDF, "crime_data1")
//    } yield writeResult
//
//    Await.result(result, 5.minutes)
//    println("Data transfer completed successfully")
//
//  } catch {
//    case e: Exception =>
//      println(s"Error: ${e.getMessage}")
//      e.printStackTrace()
//  } finally {
//    spark.stop()
//    actorSystem.terminate()
//  }
//}
