import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.crobox.clickhouse.ClickhouseClient
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object ClickhouseExample {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("clickhouse-example")

    val config =ConfigFactory.load()

    val client = new ClickhouseClient(Some(config))

    val query: Future[String] = client.query("SELECT 1")

    query.map { result =>
      println(s"Query result: $result")
    }.recover {
      case e: Exception => println(s"Query failed: ${e.getMessage}")
    }.onComplete { _ =>
      system.terminate()
    }

    Thread.sleep(5000)
  }
}