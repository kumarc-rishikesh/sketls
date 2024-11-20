package com.neu

import akka.actor.ActorSystem
import com.neu.Main.system.dispatcher
import com.neu.connectors.CKHConnector

object Main extends App {
  implicit val system: ActorSystem = ActorSystem("clickhouse-example")

  val connector = CKHConnector()

  connector.executeQuery("SELECT 1")
    .map(result => println(s"Query result: $result"))
    .onComplete(_ => system.terminate())

}