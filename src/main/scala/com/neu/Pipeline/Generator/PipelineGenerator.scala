package com.neu.Pipeline.Generator

import com.neu.Pipeline.Parser.{ConfigParser, ELParser, TransformParser}
import com.neu.Qualitycheck.QualityCheckRunner.{getResultCount, runChecks}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.concurrent.ExecutionContext
import scala.util.Try

class PipelineGenerator(
    sparkSession: SparkSession,
    ec: ExecutionContext,
    materializer: Materializer,
    actorSystem: ActorSystem
) {
  def generateJobFunctions(pipelineConfig: String): Try[List[(() => Try[Unit], String)]] = {
    ConfigParser.parsePipelineConfig(pipelineConfig).map { pipeline =>
      pipeline.pipeline.jobs.map { job =>
        val jobFunction = () => {
          for {
            // Create parsers
            transformParser <- TransformParser(job.transformation.definition)
            ipSchema         = transformParser.inputToStruct
            elParser         = ELParser(job, ipSchema)(sparkSession, ec, materializer, actorSystem)

            // Execute pipeline steps
            sourceDF       = elParser.parseSource(job.source)
            qcResults      = runChecks(sourceDF, job.quality_checks)
            _             <- Try {
                               if (getResultCount(qcResults) != qcResults.length) {
                                 throw new Exception("Quality checks failed. Aborting job")
                               }
                             }

            // Transform and write data
            opSchema       = transformParser.outputToStruct(ipSchema)
            transformedDF_ = transformParser.outputToFunction(sourceDF)
            transformedDF  = transformedDF_.select(
                               opSchema.fields.map(field => col(field.name).cast(field.dataType).as(field.name)): _*
                             )
            _             <- Try(elParser.parseDestination(job.destination, transformedDF))
          } yield ()
        }
        (jobFunction, job.trigger.value)
      }
    }
  }
}
