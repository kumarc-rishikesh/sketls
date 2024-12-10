package com.neu.Pipeline.Parser

import cats.syntax.either._
import io.circe.Error
import io.circe.generic.auto._
import io.circe.yaml.parser

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
    db: Option[String],
    query: Option[String],
    bucket: Option[String]
)

case class Transformations(
    definition: String
)

case class QualityCheck(
    `type`: String,
    rules: String
)

case class Destination(
                   `type`: String,
                   db: Option[String],
                   query: Option[String],
                   bucket: Option[String]
                 )

object ConfigParser {
  def parsePipelineConfig(yamlString: String): Either[Error, Pipeline] = {
    parser
      .parse(yamlString)
      .leftMap(err => err: Error)
      .flatMap(_.as[Pipeline])
  }
}
