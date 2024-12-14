package com.neu.Pipeline.Parser

import io.circe.generic.auto._
import io.circe.yaml.parser
import scala.util.Try

case class Pipeline(pipeline: Pipeline_)

case class Pipeline_(name: Option[String], jobs: List[Job])

case class Job(
    jobname: String,
    trigger: Trigger,
    source: Source,
    transformation: Transformations,
    quality_checks: List[QualityCheck],
    destination: Destination
)

case class Trigger(
    `type`: String,
    value: String
)

case class Source(
    `type`: String,
    table: Option[String],
    query: Option[String],
    bucket: Option[String],
    filename: Option[String]
)

case class Transformations(
    definition: String
)

case class QualityCheck(
    `type`: String,
    col: String,
    min: Option[Int],
    max: Option[Int],
    expected_type: Option[String]
)

case class Destination(
    `type`: String,
    table: Option[String],
    query: Option[String],
    bucket: Option[String],
    filename: Option[String]
)

object ConfigParser {
  def parsePipelineConfig(yamlString: String): Try[Pipeline] = {
    Try {
      parser.parse(yamlString).flatMap(_.as[Pipeline]) match {
        case Right(pipeline) => pipeline
        case Left(error)     => throw new RuntimeException(s"Failed to parse pipeline config: ${error.getMessage}")
      }
    }
  }
}
