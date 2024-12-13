package com.neu.Pipeline.Parser

import java.util.Properties
import com.neu.Connector.PGDataUtils.{readDataPG, writeDataPG}
import com.neu.Connector.{CKHActions, CKHConnector, S3Connector}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

class ELParser(
    ipSchema: StructType,
    implicit
    val sparkSession: SparkSession,
    val ec: ExecutionContext,
    val materializer: Materializer,
    val actorSystem: ActorSystem
) {

  val jdbcUrl      = "jdbc:postgresql://localhost:5432/postgres"
  val pgProperties = new Properties()
  pgProperties.setProperty("user", "postgres")
  pgProperties.setProperty("password", "1234")
  pgProperties.setProperty("driver", "org.postgresql.Driver")

  val parseSource: Source => DataFrame = source => {
    source.`type` match {
      case "clickhouse" =>
        handleClickhouseSource(source.query.getOrElse(""))
      case "s3"         =>
        handleS3Source(source.bucket.getOrElse(""), source.filename.getOrElse(""))
      case "postgres"   =>
        handlePgSource(jdbcUrl, source.table.getOrElse(""), pgProperties)
      case unknownType  =>
        throw new Exception(s"Unsupported source type: $unknownType")
    }
  }

  val parseDestination: (Destination, DataFrame) => Unit = (destination, df) => {
    destination.`type` match {
      case "clickhouse" =>
        handleClickhouseDestination(df, destination.table.getOrElse(""))
      case "s3"         =>
        handleS3Destination(df, destination.bucket.getOrElse(""), destination.filename.getOrElse(""))
      case "postgres"   =>
        handlePgDestination(df, jdbcUrl: String, destination.table.getOrElse(""), pgProperties)
      case unknownType  =>
        throw new Exception(s"Unsupported destination type: $unknownType")
    }
  }

  private def handleClickhouseSource(query: String): DataFrame = {
    val ckhConnector = CKHConnector()
    val actions      = CKHActions(sparkSession, actorSystem, ckhConnector)
    val sourceDF     = Await.result(actions.readDataCKH(query, ipSchema), 5.minutes)
    sourceDF
  }

  private def handleS3Source(bucket: String, filename: String): DataFrame = {
    val s3Connector = S3Connector()(actorSystem, ec)
    val sourceDF    = Await.result(s3Connector.readDataS3(bucket, filename, ipSchema), 5.minutes)
    sourceDF
  }

  private def handlePgSource(jdbcUrl: String, table: String, properties: Properties): DataFrame = {
    Await.result(readDataPG(sparkSession, jdbcUrl, ipSchema, table, properties), 5.minutes)
  }

  private def handleClickhouseDestination(df: DataFrame, table: String): Unit = {
    val ckhConnector = CKHConnector()
    val actions      = CKHActions(sparkSession, actorSystem, ckhConnector)
    Await.result(actions.writeDataCKH(df, table), 5.minutes)
  }

  private def handleS3Destination(df: DataFrame, bucket: String, fileName: String): Unit = {
    val s3Connector = S3Connector()(actorSystem, ec)
    Await.result(s3Connector.writeDataS3(bucket, fileName, df), 5.minutes)
  }

  private def handlePgDestination(df: DataFrame, jdbcUrl: String, table: String, pgProperties: Properties): Unit = {
    writeDataPG(df, jdbcUrl, table, pgProperties)
  }
}

object ELParser {
  def apply(job: Job, ipSchema: StructType)(
      sparkSession: SparkSession,
      ec: ExecutionContext,
      materializer: Materializer,
      actorSystem: ActorSystem
  ): ELParser = {
    new ELParser(
      ipSchema,
      sparkSession,
      ec,
      materializer,
      actorSystem
    )
  }
}
