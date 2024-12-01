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

  val connector = CKHConnector()(actorSystem, ec)
  val client = connector.client

  def writeCrimeData(): Unit = {
    val crimeData: IO[Table[CrimeData]] = Table.parseFile[Table[CrimeData]]("/home/rkc/Code/scetls/src/main/resources/london_crime_by_lsoa_trunc1.csv")
    crimeData.attempt.unsafeRunSync() match {
      case Right(table) =>
        table.foreach { crime =>
          println(crime)
          client.sink("INSERT INTO crime_data (lsoa_code, borough, major_category, minor_category, value, year, month) VALUES ", Source.single(ByteString(s"('${crime.lsoa_code}', '${crime.borough}', '${crime.major_category}', '${crime.minor_category}', ${crime.value}, ${crime.year}, ${crime.month})"))).map(result => println(result))(ec)
        }
      case Left(error) =>
        println(s"Error parsing file: $error")
    }
  }

  try {
    writeCrimeData()
  } finally {
    println("done")
    actorSystem.terminate()
  }
}