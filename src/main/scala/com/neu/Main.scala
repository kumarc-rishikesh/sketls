package com.neu

import akka.actor.{ActorSystem, Props}
import com.neu.actors.{Orchestrator, RunB}

object Main {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("orchestration-system")
    val orchestrator = system.actorOf(Props[Orchestrator], "orchestrator")

    orchestrator ! RunB
  }
}
