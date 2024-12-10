package com.neu.Pipeline.Parser

import org.apache.spark.sql.DataFrame

class ELParser (source: Source, destination: Destination) {

  val parseSource: Source => DataFrame = source => {
    source.`type` match {
      case "clickhouse" =>
        handleClickhouseSource(source.db.getOrElse(""), source.query.getOrElse(""))
      case "s3" =>
        handleS3Source(source.bucket.getOrElse(""))
      case "postgres" =>
        handlePgSource(source.db.getOrElse(""), source.query.getOrElse(""))
      case unknownType =>
        throw new Exception(s"Unsupported source type: $unknownType")
    }
  }

  val parseDestination: (Destination, DataFrame) => Unit = (destination, df) => {
    destination.`type` match {
      case "clickhouse" =>
        handleClickhouseDestination(df, destination.db.getOrElse(""), destination.query.getOrElse(""))
      case "s3" =>
        handleS3Destination(df, destination.bucket.getOrElse(""))
      case "postgres" =>
        handlePgDestination(df, destination.db.getOrElse(""), destination.query.getOrElse(""))
      case unknownType =>
        throw new Exception(s"Unsupported destination type: $unknownType")
    }
  }

  private def handleClickhouseSource(db: String, query: String): DataFrame = {
    ???
  }

  private def handleS3Source(bucket: String): DataFrame = {
    ???
  }

  private def handlePgSource(db: String, query: String): DataFrame = {
    ???
  }

  private def handleClickhouseDestination(df: DataFrame, db: String, query: String): Unit = {
    ???
  }

  private def handleS3Destination(df: DataFrame, bucket: String): Unit = {
    ???
  }

  private def handlePgDestination(df: DataFrame, db: String, query: String): Unit = {
    ???
  }
}

object ELParser {
  def apply(job:Job): ELParser = new ELParser(job.source,job.destination)
}
