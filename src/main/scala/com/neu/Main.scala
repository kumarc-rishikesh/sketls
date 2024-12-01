package com.neu

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.neu.connectors.CKHConnector
import com.phasmidsoftware.table.Table
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{Materializer, SystemMaterializer}
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object Main extends App {
  implicit val actorSystem: ActorSystem = ActorSystem("clickhouse-example")
  implicit val ec: ExecutionContext = actorSystem.dispatcher
  implicit val materializer: Materializer = SystemMaterializer(actorSystem).materializer

  val connector = CKHConnector()
  val client = connector.client

  def writeCrimeData(): Future[Unit] = {
    val crimeData: IO[Table[CrimeData]] = Table.parseFile[Table[CrimeData]]("/home/rkc/Code/scetls/src/main/resources/london_crime_by_lsoa_trunc1.csv")

    crimeData.unsafeRunSync() match {
      case table =>
        val iterable: scala.collection.immutable.Iterable[CrimeData] = table.toList

        val source = Source(iterable)

        val sink = Sink.foreachAsync(parallelism = 16) { crime: CrimeData =>
          val insertQuery = s"""
          ('${crime.lsoa_code}', '${crime.borough}', '${crime.major_category}',
           '${crime.minor_category}', ${crime.value}, ${crime.year}, ${crime.month})
          """
          val table = "crime_data"
          client.sink(s"INSERT INTO ${table} VALUES ", Source.single(ByteString(insertQuery))).map(result => println(result))
        }

        source.runWith(sink).map(_ => ())
    }
  }

  writeCrimeData().onComplete {
    case Success(_) =>
      println("All data inserted successfully.")
      actorSystem.terminate()
    case Failure(e) =>
      println(s"Failed to insert data: $e")
      actorSystem.terminate()
  }
}
