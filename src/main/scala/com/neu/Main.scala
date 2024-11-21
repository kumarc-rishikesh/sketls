package com.neu

import akka.actor.ActorSystem
import com.neu.Main.system.dispatcher
import com.neu.connectors.S3Connector
import scala.io.StdIn

object Main extends App {
  implicit val system: ActorSystem = ActorSystem("localstack-s3-example")

  val s3Connector = S3Connector()

  // Get bucket and file details from the user
  println("Enter the S3 bucket name to read the file from:")
  val readBucketName = StdIn.readLine()

  println("Enter the csv file name to read:")
  val inFileName = StdIn.readLine()

  // Read the file content from the S3 bucket
  s3Connector.readCSVFromBucket(readBucketName, inFileName)
    .map { fileContent =>
      println(s"File content read from '$inFileName':\n$fileContent")

      // Write the same content to a different file in the same or another bucket
      println("Enter the new bucket to write (or press Enter to skip):")
      val writeBucketName = StdIn.readLine()
      println("Enter the new file name to write (or press Enter to skip):")
      val newFileName = StdIn.readLine()
      if (newFileName.nonEmpty) {
        // Write the content to a new file
        s3Connector.writeCSVToBucket(writeBucketName, newFileName, fileContent)
          .map { result =>
            println(result) // Success message
          }
          .recover {
            case ex: Exception =>
              println(s"Error writing file to S3: ${ex.getMessage}")
          }
      } else {
        println("No new file name provided, skipping write operation.")
      }
    }
    .recover {
      case ex: Exception =>
        println(s"Error reading file from S3: ${ex.getMessage}")
    }
    .onComplete(_ => system.terminate())
}
