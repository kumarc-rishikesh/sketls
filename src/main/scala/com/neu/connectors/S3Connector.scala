package com.neu.connectors

import java.io.File
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.core.sync.RequestBody
import akka.actor.ActorSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}
import software.amazon.awssdk.services.s3.model._

import java.net.URI
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class S3Connector(implicit system: ActorSystem, ec: ExecutionContext) {

  private val s3Client: S3Client = S3Client.builder()
    .endpointOverride(new URI("http://localhost:4566")) // LocalStack S3 endpoint
    .region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(
      AwsBasicCredentials.create("test", "test")
    ))
    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
    .build()

  // Read CSV from S3 and load into a Spark DataFrame
  def readCSVFromBucketAsDataFrame(bucketName: String, fileName: String)
                                  (implicit spark: SparkSession): Future[DataFrame] = Future {
    val s3Uri = s"s3a://$bucketName/$fileName"
    println(s"Reading data from: $s3Uri")
    spark.read.option("header", "true").csv(s3Uri)
  }

  // Write Spark DataFrame to S3 bucket as CSV
  def writeDataFrameToBucket(bucketName: String, fileName: String, dataFrame: DataFrame): Future[Unit] = Future {
    try {
      // Ensure the bucket exists
      if (!doesBucketExist(bucketName)) createBucket(bucketName)

      // Write DataFrame to a temporary directory with overwrite mode
      val tempDir = System.getProperty("java.io.tmpdir") // Temporary directory
      val tempFilePath = s"$tempDir/dataframe-${System.currentTimeMillis()}.csv"

      // Write the DataFrame with overwrite mode
      dataFrame.write
        .option("header", "true")
        .mode("overwrite") // Overwrite existing file if it exists
        .csv(tempFilePath)

      // Upload the file to S3
      val putObjectRequest = PutObjectRequest.builder()
        .bucket(bucketName)
        .key(fileName)
        .build()

      val fileToUpload = new File(tempFilePath)
      s3Client.putObject(putObjectRequest, RequestBody.fromFile(fileToUpload))
      println(s"DataFrame successfully uploaded to s3://$bucketName/$fileName")

      // Clean up temporary file
      fileToUpload.delete()
    } catch {
      case e: Exception =>
        println(s"Error uploading DataFrame to S3: ${e.getMessage}")
        throw e
    }
  }
  // Check if the bucket exists
  private def doesBucketExist(bucketName: String): Boolean = {
    try {
      val listBucketsResponse = s3Client.listBuckets()
      listBucketsResponse.buckets().asScala.exists(_.name() == bucketName)
    } catch {
      case e: Exception =>
        println(s"Error checking bucket existence: ${e.getMessage}")
        false
    }
  }
  // Create a bucket
  private def createBucket(bucketName: String): Unit = {
    try {
      val createBucketRequest = CreateBucketRequest.builder().bucket(bucketName).build()
      s3Client.createBucket(createBucketRequest)
      println(s"Bucket '$bucketName' created.")
    } catch {
      case e: Exception =>
        println(s"Error creating bucket: ${e.getMessage}")
        throw e
    }
  }
  // Convert DataFrame to CSV as a string
  private def convertDataFrameToCSV(dataFrame: DataFrame): String = {
    val outputStream = new ByteArrayOutputStream()
    val schema = dataFrame.schema.fieldNames.mkString(",") // Header row
    val rows = dataFrame.collect().map(row => row.toSeq.mkString(",")) // Rows as CSV
    val csvContent = (Seq(schema) ++ rows).mkString("\n") // Combine header and rows
    outputStream.write(csvContent.getBytes(StandardCharsets.UTF_8))
    outputStream.toString(StandardCharsets.UTF_8.name())
  }
}

object S3Connector {
  def apply()(implicit system: ActorSystem, ec: ExecutionContext): S3Connector = new S3Connector()
}