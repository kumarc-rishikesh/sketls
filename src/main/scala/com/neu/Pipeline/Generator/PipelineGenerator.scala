package com.neu.Pipeline.Generator

import com.neu.Pipeline.Parser.{ConfigParser, ELParser, TransformParser}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.concurrent.ExecutionContext

class PipelineGenerator(
    sparkSession: SparkSession,
    ec: ExecutionContext,
    materializer: Materializer,
    actorSystem: ActorSystem
) {
  def generateJobFunctions(pipelineConfig: String): Either[String, List[(() => Unit, String)]] = {
    ConfigParser.parsePipelineConfig(pipelineConfig) match {
      case Right(pipeline) =>
        Right(
          pipeline.pipeline.jobs.map { job =>
            (
              () => {

                // Create parsers
                val transformParser = TransformParser(job.transformation.definition).getOrElse(
                  throw new Exception("Failed to parse transformation")
                )
                val ipSchema        = transformParser.inputToStruct
                val elParser        = ELParser(job, ipSchema)(sparkSession, ec, materializer, actorSystem)
                // Execute pipeline steps
                val sourceDF        = elParser.parseSource(job.source)
                val opSchema        = transformParser.outputToStruct(ipSchema)
                val transformedDF_  = transformParser.outputToFunction(sourceDF)
                val transformedDF   = transformedDF_.select(
                  opSchema.fields.map(field => col(field.name).cast(field.dataType).as(field.name)): _*
                )
                elParser.parseDestination(job.destination, transformedDF)
              },
              job.trigger.value
            )
          }
        )
      case Left(error)     =>
        Left(s"Failed to parse pipeline config: $error")
    }
  }
}
