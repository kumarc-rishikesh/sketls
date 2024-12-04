package com.neu

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
