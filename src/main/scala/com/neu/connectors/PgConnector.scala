package com.neu.connectors

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape
import scala.concurrent.ExecutionContext.global
import scala.concurrent.Future

object PgConnector {

  def main(args: Array[String]) = {
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "slick-akka-system")

   try {
     val db = Database.forConfig("myPGDB")
     implicit val executionContext = system.executionContext


     class Crime(tag: Tag) extends Table[(String, String, String, String, Int, Int, Int)](tag, "london_crime"){
       def lsoa_code = column[String]("lsoa_code")
       def borough = column[String]("borough")
       def major_category =column[String]("major_category")
       def minor_category =column[String]("minor_category")
       def _value = column[Int]("value")
       def _year = column[Int]("year")
       def _month = column[Int]("month")

       override def * : ProvenShape[(String, String, String, String, Int, Int, Int)] =
         (lsoa_code, borough, major_category, minor_category, _value, _year, _month)
     }

     val crimes = TableQuery[Crime]
     val queryLimited = crimes.take(1000)

     val program : DBIO[Seq[(String, String, String, String, Int, Int, Int)]] = queryLimited.result

     val futureResult: Future[Seq[(String, String, String, String, Int, Int, Int)]] =
       db.run(program)

     futureResult.onComplete {result =>

       println(result)
       db.close() // Close the database connection when done
       system.terminate()
     }(global)

   } catch  {
     case e: Throwable =>
       println(s"Error: ${e.getMessage}")
       e.printStackTrace()
       system.terminate()
   }
  }
}
