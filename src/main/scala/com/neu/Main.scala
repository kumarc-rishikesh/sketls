package com.neu

import com.neu.connectors.CKHConnector
import CrimeData._
import akka.actor.ActorSystem
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.phasmidsoftware.table.Table
import org.apache.pekko.util.ByteString
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.ExecutionContext

object Main extends App {
  implicit val actorSystem: ActorSystem = ActorSystem("clickhouse-example")
  implicit val ec: ExecutionContext = actorSystem.dispatcher

  val connector = CKHConnector()
  val client = connector.client

  def writeCrimeData(): Unit = {
    val crimeData: IO[Table[CrimeData]] = Table.parseFile[Table[CrimeData]]("/home/rkc/Code/scetls/src/main/resources/london_crime_by_lsoa_trunc.csv")
    crimeData.unsafeRunSync() match {
      case table =>
        table.foreach { crime =>
          val insertQuery = s"""
          ('${crime.lsoa_code}', '${crime.borough}', '${crime.major_category}',
           '${crime.minor_category}', ${crime.value}, ${crime.year}, ${crime.month})
          """
          client.sink("INSERT INTO crime_data VALUES ",Source.single(ByteString(insertQuery)))
          Thread.sleep(2)
        }
    }
  }

  try {
    writeCrimeData()
  } finally {
    println("done")
    actorSystem.terminate()
  }
}