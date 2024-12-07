package Parsers

import io.circe.yaml.parser
import io.circe.{Error, ParsingFailure}
import io.circe.generic.auto._
import cats.syntax.either._

case class Pipeline(pipeline: Pipeline_)

case class Pipeline_(name: Option[String], jobs: List[Job])

case class Job(
    jobname: String,
    trigger: List[Trigger],
    sources: List[Source],
    transformations: List[Transformations],
    quality_checks: List[QualityCheck],
    destinations: Option[List[Destination]],
    monitoring: Option[Monitoring]
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
    bucket: String,
    format: String
)

case class Monitoring(
    prometheus: PrometheusConfig
)

case class PrometheusConfig(
    enabled: Boolean,
    metrics: List[String]
)

object ConfigParser {
  def parsePipelineConfig(yamlString: String): Either[Error, Pipeline] = {
    parser
      .parse(yamlString)
      .leftMap(err => err: Error)
      .flatMap(_.as[Pipeline])
  }
}
