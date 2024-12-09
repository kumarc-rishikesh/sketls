package com.neu.connectors

import akka.actor.ActorSystem
import org.apache.spark.sql.SparkSession
import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn

object S3LocalstackActions {

  def performS3Operations()(implicit system: ActorSystem, ec: ExecutionContext, s3Connector: S3Connector, spark: SparkSession): Future[Unit] = {
    // Get bucket and file details from the user
    println("Enter the S3 bucket name to read the file from:")
    val readBucketName = StdIn.readLine()

    println("Enter the CSV file name to read:")
    val inFileName = StdIn.readLine()

    // Read the file content into a DataFrame
    s3Connector.readCSVFromBucketAsDataFrame(readBucketName, inFileName)
      .flatMap { dataFrame =>
        println(s"Data read from '$inFileName':")
        dataFrame.show() // Display sample rows

        // Ask for details to write the content to a different file
        println("Enter the new bucket to write (or press Enter to skip):")
        val writeBucketName = StdIn.readLine()

        println("Enter the new file name to write (or press Enter to skip):")
        val newFileName = StdIn.readLine()

        if (newFileName.nonEmpty) {
          // Write the DataFrame to the new file
          s3Connector.writeDataFrameToBucket(writeBucketName, newFileName, dataFrame)
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