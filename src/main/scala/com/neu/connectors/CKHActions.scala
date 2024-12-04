package com.neu.connectors

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.neu.CrimeData
import com.phasmidsoftware.table.Table
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{Materializer, SystemMaterializer}
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString

import scala.concurrent.{ExecutionContext, Future}

class CKHActions(actorSystem: ActorSystem, ckhConnector: CKHConnector) {
  private val ckh_client = ckhConnector.client
  implicit val ec: ExecutionContext = actorSystem.dispatcher
  implicit val materializer: Materializer = SystemMaterializer(actorSystem).materializer

  def writeCrimeData(): Future[Unit] = {
    val crimeData: IO[Table[CrimeData]] = Table.parseFile[Table[CrimeData]]("/home/rkc/Code/scetls/src/main/resources/london_crime_by_lsoa_trunc1.csv")

    val table = "crime_data"

    crimeData.unsafeRunSync().toList.grouped(1000).foldLeft(Future.successful(())) {
      (acc, batch) =>
        acc.flatMap { _ =>
          val batchValues = batch.map { crime =>
            s"""('${crime.lsoa_code}', '${crime.borough}', '${crime.major_category}',
               |'${crime.minor_category}', ${crime.value}, ${crime.year}, ${crime.month})""".stripMargin
          }.mkString(",\n")

          ckh_client.sink(
            s"INSERT INTO $table VALUES ",
            Source.single(ByteString(batchValues))
          ).map(result => println(s"Batch inserted"))
        }
    }
  }
}

object CKHActions {
  def apply(actorSystem: ActorSystem, ckhConnector: CKHConnector): CKHActions = {
    new CKHActions(actorSystem, ckhConnector)
  }

  def writeCrimeData(actorSystem: ActorSystem, ckhConnector: CKHConnector): Future[Unit] = {
    new CKHActions(actorSystem, ckhConnector).writeCrimeData()
  }
}