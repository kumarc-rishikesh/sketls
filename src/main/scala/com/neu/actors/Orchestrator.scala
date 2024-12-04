package com.neu.actors

import akka.actor.{Actor, ActorSystem, Props}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global // For scheduling

case object RunA
case object RunB
case class BResult(value: Int)

class Orchestrator extends Actor{

  override def receive: Receive = {
    case RunB =>
      var bResult = b() // Execute function b
      self ! BResult(bResult) // Send the result back to self

    case BResult(value) =>
      if (value > 8) {
        a() // Execute function a
        // Schedule RunB after 5 minutes
        context.system.scheduler.scheduleOnce(5.minutes, self, RunB)
      } else {
        // Handle the case where b's condition is not met
        println("b's condition not met. Performing alternative action...")
        // ... logic for other jobs depending on a ...
        // You might want to reschedule RunB or take other actions.
        context.system.scheduler.scheduleOnce(10.seconds, self, RunB) // Reschedule anyway
      }

    case RunA =>
      a()

  }

  def b(): Int = {
    println("fun 1")
    Thread.sleep(5000) // Simulate a 5-second delay
    6
  }

  def a(): Unit = {
    println("ok")
  }
}
