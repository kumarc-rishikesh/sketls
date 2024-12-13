package com.neu.Connector

import org.apache.pekko.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.crobox.clickhouse.ClickhouseClient

class CKHConnector() {
  private val config = ConfigFactory.load()
  val client         = new ClickhouseClient(Some(config))
}

object CKHConnector {
  def apply(): CKHConnector = {
    new CKHConnector()
  }
}
