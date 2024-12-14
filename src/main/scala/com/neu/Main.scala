package com.neu

import com.neu.Pipeline.Generator.PipelineGenerator
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{Materializer, SystemMaterializer}
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

object Main extends App {
  println("Hello world")
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("scetls")
    .config("spark.master", "local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .getOrCreate()

  implicit val actorSystem: ActorSystem   = ActorSystem("scetls")
  implicit val ec: ExecutionContext       = actorSystem.dispatcher
  implicit val materializer: Materializer = SystemMaterializer(actorSystem).materializer

  val configTry = Try {
    val source = scala.io.Source.fromFile("./docs/sample_config.yaml")
    try {
      source.mkString
    } finally {
      source.close()
    }
  }

  val generator = new PipelineGenerator(spark, ec, materializer, actorSystem)

  val result = for {
    yamlString <- configTry
    jobMap     <- generator.generateJobFunctions(yamlString)
    _          <- Try {
                    jobMap.foreach { case (jobFunction, _) =>
                      jobFunction() match {
                        case Success(_)         => println("Job completed successfully")
                        case Failure(exception) => println(s"Job failed: ${exception.getMessage}")
                      }
                    }
                  }
  } yield ()

  result match {
    case Success(_)         => println("All jobs completed")
    case Failure(exception) => println(s"Error: ${exception.getMessage}")
  }

  // Ensure to close resources
  spark.stop()
  actorSystem.terminate()
}
