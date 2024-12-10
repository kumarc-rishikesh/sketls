package com.neu.Pipeline.Generator

import com.neu.Pipeline.Parser.{ConfigParser, ELParser, TransformParser}
import org.apache.spark
import org.apache.spark.sql.functions.col



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
              val ipSchema = transformParser.inputToStruct
              val opSchema = transformParser.outputToStruct(ipSchema)
              val transformedDF_ = transformParser.outputToFunction(sourceDF)
              val transformedDF = transformedDF_.select(
                opSchema.fields.map(field =>
                  col(field.name).cast(field.dataType).as(field.name)
                ): _*
              )
              elParser.parseDestination(job.destination, transformedDF)
            }
          }
        )
      case Left(error) =>
        Left(s"Failed to parse pipeline config: $error")
    }
  }
}

