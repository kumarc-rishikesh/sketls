package com.neu.connectors

import org.apache.pekko.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.stream.ClickhouseSink

import scala.concurrent.{ExecutionContext, Future}

class CKHConnector(implicit system: ActorSystem, ec: ExecutionContext) {
  private val config = ConfigFactory.load()
  val client = new ClickhouseClient(Some(config))

  val sink = ClickhouseSink.toSink(config, client)
  def executeQuery(query: String): Future[String] = {
    client.query(query)
  }
}

object CKHConnector {
  def apply()(implicit system: ActorSystem, ec: ExecutionContext): CKHConnector = {
    new CKHConnector()
  }
}