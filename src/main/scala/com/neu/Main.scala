package com.neu

import akka.actor.ActorSystem
import com.neu.connectors.{S3Connector, S3LocalstackActions}
import scala.concurrent.ExecutionContext
import com.neu.connectors.CKHConnector
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{Materializer, SystemMaterializer}
import connectors.CKHActions
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object Main extends App {
  implicit val actorSystem: ActorSystem = ActorSystem("clickhouse-example")
  implicit val ec: ExecutionContext = actorSystem.dispatcher
  implicit val materializer: Materializer = SystemMaterializer(actorSystem).materializer
  
  implicit val system: ActorSystem = ActorSystem("localstack-s3-example")
  implicit val ec: ExecutionContext = system.dispatcher

  val s3Connector = S3Connector()

  S3LocalstackActions.performS3Operations()(system, ec, s3Connector)
    .onComplete(_ => system.terminate())

  val ckhConnector = CKHConnector()

  val result = CKHActions.writeCrimeData(actorSystem, ckhConnector)

  result.onComplete {
    case Success(_) =>
      println("All data inserted to clickhouse successfully.")
      actorSystem.terminate()
    case Failure(e) =>
      println(s"Failed to insert data to clickhouse: $e")
      actorSystem.terminate()
  }
}
