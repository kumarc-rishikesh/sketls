package com.neu

import akka.actor.{ActorSystem, Props}
import com.neu.actors.{Orchestrator, Run_placeholder_readDataPG}

object Main {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("orchestration-system")
    val orchestratorRef = system.actorOf(Props[Orchestrator], "orchestrator")

    orchestratorRef ! Run_placeholder_readDataPG
  }
}
