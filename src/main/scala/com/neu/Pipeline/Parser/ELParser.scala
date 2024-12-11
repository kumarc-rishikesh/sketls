package com.neu.Pipeline.Parser

import com.neu.Main.actorSystem
import com.neu.connectors.{CKHActions, CKHConnector, S3Connector}
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
  val parseSource: Source => DataFrame = source => {
    source.`type` match {
      case "clickhouse" =>
        handleClickhouseSource(source.query.getOrElse(""))
      case "s3"         =>
        handleS3Source(source.bucket.getOrElse(""), source.filename.getOrElse(""))
//      case "postgres"   =>
//        handlePgSource(source.db.getOrElse(""), source.query.getOrElse(""))
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
//      case "postgres"   =>
//        handlePgDestination(df, destination.table.getOrElse(""), destination.query.getOrElse(""))
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

//  private def handlePgSource(db: String, query: String): DataFrame = {
//    ???
//  }

  private def handleClickhouseDestination(df: DataFrame, table: String): Unit = {
    val ckhConnector = CKHConnector()
    val actions      = CKHActions(sparkSession, actorSystem, ckhConnector)
    Await.result(actions.writeDataCKH(df, table), 5.minutes)
  }

  private def handleS3Destination(df: DataFrame, bucket: String, fileName: String): Unit = {
    val s3Connector = S3Connector()(actorSystem, ec)
    Await.result(s3Connector.writeDataS3(bucket, fileName, df), 5.minutes)
  }

//  private def handlePgDestination(df: DataFrame, db: String, query: String): Unit = {
//    ???
//  }
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
