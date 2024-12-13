package com.neu

// import com.neu.qualitycheck.QualityCheckRunner
// import org.apache.spark.sql.SparkSession

// object Main {
//   def main(args: Array[String]): Unit = {
//     val spark = SparkSession.builder()
//       .appName("Quality Checks Example")
//       .master("local[*]")
//       .getOrCreate()

//     val data = spark.read.option("header", "true").csv("london_crime_by_lsoa_sample.csv")

//     // Example YAML-like Quality Check Config
//     val qualityCheckConfig = Seq(
//       Map("type" -> "null_check", "column" -> "borough"),
//       Map("type" -> "type_check", "column" -> "year", "expected_type" -> "int"),
//       Map("type" -> "range_check", "column" -> "values", "min" -> "0", "max" -> "1000"),
//       Map("type" -> "uniqueness_check", "column" -> "lsoa_code")
//     )

//     // Run Quality Checks
//     val results = QualityCheckRunner.runChecks(data, qualityCheckConfig)

//     // Display Results
//     results.foreach { result =>
//       println(s"Check: ${result.checkName}, Result of check: ${result.passed}")
//       result.failedRecords.foreach { records =>
//         println(s"Failed Records: ${records.mkString("\n")}")
//       }
//     }

//     spark.stop()

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
