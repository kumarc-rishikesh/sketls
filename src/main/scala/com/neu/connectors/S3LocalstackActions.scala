package com.neu.connectors

import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn

object S3LocalstackActions {

  def performS3Operations()(implicit system: ActorSystem, ec: ExecutionContext, s3Connector: S3Connector): Future[Unit] = {
    // Get bucket and file details from the user
    println("Enter the S3 bucket name to read the file from:")
    val readBucketName = StdIn.readLine()

    println("Enter the CSV file name to read:")
    val inFileName = StdIn.readLine()

    // Read the file content from the S3 bucket
    s3Connector.readCSVFromBucket(readBucketName, inFileName)
      .flatMap { fileContent =>
        println(s"File content read from '$inFileName':\n$fileContent")

        // Ask for details to write the content to a different file
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
        } else {
          Future.successful(println("No new file name provided, skipping write operation."))
        }
      }
      .recover {
        case ex: Exception =>
          println(s"Error during S3 operations: ${ex.getMessage}")
      }
  }
}