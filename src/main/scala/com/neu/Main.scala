package com.neu

import akka.actor.ActorSystem
import com.neu.connectors.{S3Connector, S3LocalstackActions}
import scala.concurrent.ExecutionContext

object Main extends App {
  implicit val system: ActorSystem = ActorSystem("localstack-s3-example")
  implicit val ec: ExecutionContext = system.dispatcher

  val s3Connector = S3Connector()

  S3LocalstackActions.performS3Operations()(system, ec, s3Connector)
    .onComplete(_ => system.terminate())
}
