package com.neu.connectors

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString
import org.apache.spark.sql.types.StructType

import scala.concurrent.{ExecutionContext, Future}

class CKHActions(sparkSession: SparkSession, actorSystem: ActorSystem, ckhConnector: CKHConnector) {
  private val ckh_client                  = ckhConnector.client
  implicit val ec: ExecutionContext       = actorSystem.dispatcher
  implicit val materializer: Materializer = materializer

  def writeDataCKH(df: DataFrame, tableName: String): Future[Unit] = {
    val rows = df.collect()

    rows.grouped(1000).foldLeft(Future.successful(())) { (acc, batch) =>
      acc.flatMap { _ =>
        val batchValues = batch
          .map { row =>
            println(row)
            val values = row.toSeq.map {
              case null      => "NULL"
              case s: String => s"'${s.replace("'", "''")}'" // Escape single quotes
              case n: Number => n.toString
              case value     => s"'${value.toString}'"
            }
            s"(${values.mkString(", ")})"
          }
          .mkString(",\n")

        if (batchValues.nonEmpty) {
          ckh_client
            .sink(
              s"INSERT INTO $tableName VALUES ",
              Source.single(ByteString(batchValues))
            )
            .map(_ => println("Batch inserted"))
        } else {
          Future.successful(())
        }
      }
    }
  }

  def readDataCKH(query: String, schema: StructType): Future[DataFrame] = {
    ckhConnector.client
      .source(query)
      .runWith(Sink.seq)
      .map { results =>
        sparkSession.read
          .schema(schema)
          .option("header", "false")
          .csv(sparkSession.createDataset(results)(Encoders.STRING))
      }
  }
}

object CKHActions {
  def apply(sparkSession: SparkSession, actorSystem: ActorSystem, ckhConnector: CKHConnector): CKHActions = {
    new CKHActions(sparkSession, actorSystem, ckhConnector)
  }

  def writeDataCKH(
      sparkSession: SparkSession,
      actorSystem: ActorSystem,
      ckhConnector: CKHConnector,
      df: DataFrame,
      tableName: String
  ): Future[Unit] = {
    new CKHActions(sparkSession, actorSystem, ckhConnector).writeDataCKH(df, tableName)
  }

  def readDataCKH(
      sparkSession: SparkSession,
      actorSystem: ActorSystem,
      ckhConnector: CKHConnector,
      query: String,
      schema: StructType
  ): Future[DataFrame] = {
    new CKHActions(sparkSession, actorSystem, ckhConnector).readDataCKH(query, schema)
  }
}
