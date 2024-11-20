package com.neu.connectors

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.crobox.clickhouse.ClickhouseClient
import scala.concurrent.{ExecutionContext, Future}

class CKHConnector(implicit system: ActorSystem, ec: ExecutionContext) {
  private val config = ConfigFactory.load()
  private val client = new ClickhouseClient(Some(config))

  def executeQuery(query: String): Future[String] = {
    client.query(query)
  }
}

object CKHConnector {
  def apply()(implicit system: ActorSystem, ec: ExecutionContext): CKHConnector = {
    new CKHConnector()
  }
}