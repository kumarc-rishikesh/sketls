package com.neu.Pipeline.Generator

import com.neu.Pipeline.Parser.{ConfigParser, ELParser, TransformParser}

class PipelineGenerator {
  def generateJobFunctions(pipelineConfig: String): Either[String, List[() => Unit]] = {
    ConfigParser.parsePipelineConfig(pipelineConfig) match {
      case Right(pipeline) =>
        Right(
          pipeline.pipeline.jobs.map { job =>
            () => {
              // Create parsers
              val elParser = ELParser(job)
              val transformParser = TransformParser(job.transformation.definition).getOrElse(
                throw new Exception("Failed to parse transformation")
              )

              // Execute pipeline steps
              val sourceDF = elParser.parseSource(job.source)
              val transformedDF = transformParser.outputToFunction(sourceDF)
              elParser.parseDestination(job.destination, transformedDF)
            }
          }
        )
      case Left(error) =>
        Left(s"Failed to parse pipeline config: $error")
    }
  }
}

