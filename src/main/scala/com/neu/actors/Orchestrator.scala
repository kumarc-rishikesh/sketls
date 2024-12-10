package com.neu.actors

import akka.actor.{Actor, ActorSystem, Props}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global // For scheduling

case object Run_placeholder_readDataPG
case object Run_placeholder_writeDataPG
case class RunStep(step: Int)


class Orchestrator extends Actor{

  override def receive: Receive = {


    case Run_placeholder_readDataPG =>
      var result_readDataPG = placeholder_readDataPG() // Execute function b
      self ! RunStep(result_readDataPG) // Send the result back to self

    case RunStep(step) =>
      if (step > 8) {
        placeholder_readDataPG() // Execute function a
        // Schedule RunB after 5 minutes
        context.system.scheduler.scheduleOnce(15.seconds, self, Run_placeholder_readDataPG)
      } else {

        // Handle the case where b's condition is not met
        println("Read Data PG Case's condition not met. Performing alternative action...")

        // You might want to reschedule RunB or take other actions.
        context.system.scheduler.scheduleOnce(20.seconds, self, Run_placeholder_readDataPG) // Reschedule anyway
      }

    case Run_placeholder_writeDataPG =>
      placeholder_writeDataPG()
  }

  private def placeholder_readDataPG(): Int = {
    println("Placeholder Read Data PG")
    Thread.sleep(5000) // Simulate a 5-second delay
    5
  }

  private def placeholder_writeDataPG(): Unit = {
    println("Placeholder Write Data PG")
  }
}

