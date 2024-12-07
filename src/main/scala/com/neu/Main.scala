package com.neu

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{Materializer, SystemMaterializer}
import org.apache.spark.sql.SparkSession
import com.neu.connectors.{CKHActions, CKHConnector}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object Main extends App {
  implicit val actorSystem: ActorSystem = ActorSystem("clickhouse-example")
  implicit val ec: ExecutionContext = actorSystem.dispatcher
  implicit val materializer: Materializer = SystemMaterializer(actorSystem).materializer

  val spark = SparkSession.builder()
    .appName("scetls")
    .master("local[*]")
    .getOrCreate()

  val schema = CrimeData.schema
  val ckhConnector = CKHConnector()
  val actions = CKHActions(spark, actorSystem, ckhConnector)

  try {
    // Chain operations using flatMap
    val result = for {
      _ <- Future(println("Reading data into dataferame..."))
      sourceDF <- actions.readDataCKH("SELECT * FROM crime_data FORMAT CSV", schema)

      _ <- Future(println(s"Read ${sourceDF.count()} rows from crime_data"))

      _ <- Future(println("Writing data to datastore..."))
      writeResult <- actions.writeDataCKH(sourceDF, "crime_data1")
    } yield writeResult

    Await.result(result, 5.minutes)
    println("Data transfer completed successfully")

  } catch {
    case e: Exception =>
      println(s"Error: ${e.getMessage}")
      e.printStackTrace()
  } finally {
    spark.stop()
    actorSystem.terminate()
  }
}
