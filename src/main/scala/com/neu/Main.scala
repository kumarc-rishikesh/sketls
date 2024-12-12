package com.neu

// import akka.actor.ActorSystem
// //import akka.http.impl.util.StreamUtils.OnlyRunInGraphInterpreterContext.executionContext
// //import com.neu.Main.materializer.executionContext
// import com.neu.connectors.{S3Connector, S3LocalstackActions}

// import scala.concurrent.ExecutionContext
// import com.neu.connectors.CKHConnector
// import org.apache.pekko.actor.ActorSystem
// import org.apache.pekko.stream.{Materializer, SystemMaterializer}
// import org.apache.spark.sql.SaveMode
// //import connectors.CKHActions
// import org.apache.spark.sql.SparkSession

// import java.util.Properties
// import scala.concurrent.{ExecutionContext, Future}
// import scala.util.{Failure, Success}
// import com.neu.connectors.PGDataUtils


// object Main extends App{
//   implicit val actorSystem: ActorSystem = ActorSystem("clickhouse-example")
//   implicit val ec: ExecutionContext = actorSystem.dispatcher
//   //implicit val ec: ExecutionContext = ExecutionContext.global

//   implicit val materializer: Materializer = SystemMaterializer(actorSystem).materializer
  
//   implicit val system: ActorSystem = ActorSystem("localstack-s3-example")
//   implicit val ec: ExecutionContext = system.dispatcher
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


//   val spark = SparkSession.builder()
//     .appName("scetls")
//     .master("local[*]") // Or your Spark cluster configuration
//     .getOrCreate()

//   val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
//   val tableNameToRead = "london_crime_trunc"  // Table to read from
//   val tableNameToWrite = "london_crime_write" // Table to write to

//   val properties = new Properties()
//   properties.setProperty("user", "postgres")
//   properties.setProperty("password", "1234")
//   properties.setProperty("driver", "org.postgresql.Driver")
//   import scala.concurrent.ExecutionContext.Implicits.global

//   val readFuture = PGDataUtils.readDataPG(spark, jdbcUrl, tableNameToRead, properties)
//   readFuture.onComplete {
//     case Success(df) =>
//       println("Data read successfully:")
//       df.show()

//       // Example transformation (optional):
//       val processedDF = df.select("lsoa_code") // Select specific columns, if needed
//       val writeFuture = PGDataUtils.writeDataPG(df, jdbcUrl, tableNameToWrite, properties) // Using COPY

//       writeFuture.onComplete {
//         case Success(_) =>
//           println("Data written successfully.")
//           spark.stop() // Stop Spark after writing
//         case Failure(e) =>
//           val mess = Option(e.getMessage).getOrElse("No message available")
//           println(s"Error writing data: ${e.getMessage}")
//           spark.stop() // Stop Spark even if there's an error
//       }

//     case Failure(e) =>
//       println(s"Error reading data: ${e.getMessage}")
//       spark.stop() // Stop Spark if reading fails
//   }
// }
