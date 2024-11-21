package com.neu

import akka.actor.ActorSystem
import com.neu.Main.system.dispatcher
import com.neu.connectors.CKHConnector
import CrimeData._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.phasmidsoftware.table.Table
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

object Main extends App {
  implicit val system: ActorSystem = ActorSystem("clickhouse-example")
  val client = CKHConnector().client

  def writeCrimeData(): Unit = {
    val crimeData: IO[Table[CrimeData]] = Table.parseFile[Table[CrimeData]]("/home/rkc/Code/scetls/src/main/resources/london_crime_by_lsoa_trunc.csv")
    var a =0
    crimeData.unsafeRunSync() match {
      case table =>
        table.foreach { crime =>
          val insertQuery = s"""
          ('${crime.lsoa_code}', '${crime.borough}', '${crime.major_category}',
           '${crime.minor_category}', ${crime.value}, ${crime.year}, ${crime.month})
          """
          client.sink("INSERT INTO crime_data VALUES ",Source.single(ByteString(insertQuery))).map(result => println(result))
        }
    }
  }

  try {
    writeCrimeData()
  } finally {
    println("done")
    system.terminate()
  }
}