package com.neu.connectors

import akka.actor.ActorSystem
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}
import software.amazon.awssdk.services.s3.model._

import scala.concurrent.{ExecutionContext, Future}
import java.net.URI
import scala.jdk.CollectionConverters.CollectionHasAsScala

class S3Connector(implicit system: ActorSystem, ec: ExecutionContext) {

  // Configure LocalStack S3 client
  private val s3Client: S3Client = S3Client.builder()
    .endpointOverride(new URI("http://localhost:4566"))
    .region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(
      AwsBasicCredentials.create("test", "test")
    ))
    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
    .build()

  // Read CSV from a bucket
  def readCSVFromBucket(bucketName: String, fileName: String): Future[String] = Future {
    val getObjectRequest = GetObjectRequest.builder()
      .bucket(bucketName)
      .key(fileName)
      .build()

    // Get the file from S3
    val response = s3Client.getObject(getObjectRequest)
    scala.io.Source.fromInputStream(response).mkString
  }

  // Write CSV to a bucket
  def writeCSVToBucket(bucketName: String, fileName: String, content: String): Future[String] = Future {
    // Check if the bucket exists, and if not, create it
    try {
      val listBucketsResponse = s3Client.listBuckets()
      val bucketExists = listBucketsResponse.buckets().asScala.exists(_.name() == bucketName)

      if (!bucketExists) {
        val createBucketRequest = CreateBucketRequest.builder()
          .bucket(bucketName)
          .build()
        s3Client.createBucket(createBucketRequest)
        println(s"Bucket '$bucketName' created.")
      }

      // Upload the file content after ensuring the bucket exists
      val putObjectRequest = PutObjectRequest.builder()
        .bucket(bucketName)
        .key(fileName)
        .build()

      s3Client.putObject(putObjectRequest, RequestBody.fromString(content))

      s"File '$fileName' uploaded to bucket '$bucketName'."
    } catch {
      case ex: Exception =>
        throw new RuntimeException(s"Error while checking or creating bucket: ${ex.getMessage}")
    }
  }
}

object S3Connector {
  def apply()(implicit system: ActorSystem, ec: ExecutionContext): S3Connector = {
    new S3Connector()
  }
}