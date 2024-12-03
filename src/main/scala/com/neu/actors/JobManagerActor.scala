package com.neu.actors
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream.stage.GraphStageLogic.StageActor
import io.circe.generic.auto._
import io.circe.yaml.parser

case class StartJob(config: JobConfig)
case class StageCompleted(stageName: String)
case class JobFailed(reason: String)
case class StartStage()

// JobConfig was never initialized

class JobManagerActor extends Actor{
  private var currentStage: String = _
  private var stageActors: Map[String, ActorRef] = _

  override def receive: Receive = {

    case StartJob(config) =>
      stageActors = createStageActors(config)
      currentStage = config.pipeline.stages.head.name
      stageActors(currentStage) ! StartStage

  }

  private def createStageActors(config: JobConfig, currentState: String): Map[String, ActorRef] = {
    config.pipeline.stages.map {
      stage =>
        val actorName = stage.name
        val props = Props(new StageActor(stage))
        val actorRef = context.actorOf(props, actorName)
        (actorName -> actorRef)
    }.toMap
  }

  private def getNextStage(config: JobConfig, currentStage: String): String = {
    ???
  }
}

case class StageActor(stateConfig: Stage) extends Actor {

  override def receive: Receive = {
    case StartStage =>
      val result = performStageTask(stateConfig)
      sender()
  }

  private def performStageTask(stageConfig: Stage): Any = {
    // ... your stage logic here ...
    // Example:
    if (stageConfig.`type` == "Extract") {
      // ... extraction logic ...
    } else if (stageConfig.`type` == "Transform") {
      // ... transformation logic ...
    } else { // Load
      // ... loading logic ...
    }
  }
}

sealed trait DataSource
case class PostgresqlSource(connectionString: String, query: String) extends DataSource
case class CsvDestination(path: String) extends DataSource
// Stage definition
case class Stage(
                  name: String,
                  `type`: String, // Extract, Transform, Load
                  source: Option[DataSource] = None,
                  destination: Option[DataSource] = None,
                  function: Option[String] = None, // For transform stages
                  next_stage: Option[String] = None
                )
case class Pipeline(stages: List[Stage])

case class JobConfig(job_name: String, pipeline: Pipeline)

object ConfigLoader {
  def loadConfig(yamlString: String): Either[io.circe.Error, JobConfig] = {
    parser.parse(yamlString).flatMap(_.as[JobConfig])
  }
}