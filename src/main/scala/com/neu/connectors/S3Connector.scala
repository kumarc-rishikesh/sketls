package com.neu.connectors

import org.apache.pekko.actor.ActorSystem
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.io.File
import java.net.URI
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class S3Connector(implicit system: ActorSystem, ec: ExecutionContext) {
  private val s3Client: S3Client = S3Client.builder()
    .endpointOverride(new URI("http://localhost:4566"))
    .region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(
      AwsBasicCredentials.create("test", "test")
    ))
    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
    .build()

  // Read CSV from S3 and load into a Spark DataFrame
  def readDataS3(bucketName: String, fileName: String, schema: StructType)
                                  (implicit spark: SparkSession): Future[DataFrame] = Future {
    val s3Uri = s"s3a://$bucketName/$fileName"
    println(s"Reading data from: $s3Uri")
    spark.read.schema(schema).option("header", "true").csv(s3Uri)
  }

  // Write Spark DataFrame to S3 bucket as CSV
  def writeDataS3(bucketName: String, fileName: String, dataFrame: DataFrame): Future[Unit] = Future {
    try {
      if (!doesBucketExist(bucketName)) createBucket(bucketName)

      // Create a temporary file instead of directory
      val tempFile = File.createTempFile("dataframe-", ".csv")
      tempFile.deleteOnExit()

      // Write DataFrame directly to a single CSV file
      dataFrame.coalesce(1) // Ensure single file output
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(tempFile.getParent + "/output")

      // Find the actual CSV file in the output directory
      val outputDir = new File(tempFile.getParent + "/output")
      val csvFile = outputDir.listFiles().find(_.getName.endsWith(".csv")).get

      // Upload to S3
      val putObjectRequest = PutObjectRequest.builder()
        .bucket(bucketName)
        .key(fileName)
        .contentType("text/csv")
        .build()

      s3Client.putObject(putObjectRequest, RequestBody.fromFile(csvFile))

      // Cleanup
      csvFile.delete()
      outputDir.delete()
      tempFile.delete()
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
}

object S3Connector {
  def apply()(implicit system: ActorSystem, ec: ExecutionContext): S3Connector = new S3Connector()
}