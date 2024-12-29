package com.neu.Connector

import org.apache.pekko.actor.ActorSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.ExecutionContext

class S3ConnectorSpec extends AnyFlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  implicit val system: ActorSystem  = ActorSystem("S3ConnectorSpec")
  implicit val ec: ExecutionContext = system.dispatcher

  // Configure patience for async operations
  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("S3ConnectorSpec")
    .config("spark.master", "local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .getOrCreate()

  val s3Connector: S3Connector = S3Connector()(system, ec)

  val testBucketName = "test-bucket"
  val testFileName   = "test-file.csv"

  override def afterAll(): Unit = {
    sparkSession.stop()
    system.terminate()
  }

  "S3Connector" should "read data from S3 with correct schema" in {
    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable = true),
        StructField("name", StringType, nullable = true)
      )
    )

    // Create test data and write it to S3 first
    val testData = Seq(("1", "John"), ("2", "Jane"))
    val testDf   = sparkSession.createDataFrame(testData).toDF("id", "name")

    // Write test data
    whenReady(s3Connector.writeDataS3(testBucketName, testFileName, testDf)) { _ =>
      // Then read it back
      whenReady(s3Connector.readDataS3(testBucketName, testFileName, schema)) { resultDf =>
        // Verify schema
        resultDf.schema shouldBe schema

        // Verify data
        val rows = resultDf.collect()
        rows.length shouldBe 2
        rows(0).getString(0) shouldBe "1"
        rows(0).getString(1) shouldBe "John"
        rows(1).getString(0) shouldBe "2"
        rows(1).getString(1) shouldBe "Jane"
      }
    }
  }

  it should "write DataFrame to S3 successfully" in {
    val testData = Seq(("3", "Alice"), ("4", "Bob"))
    val df       = sparkSession.createDataFrame(testData).toDF("id", "name")

    val writeResult = s3Connector.writeDataS3(testBucketName, "output.csv", df)

    whenReady(writeResult) { _ =>
      // Verify the file exists in S3 by trying to read it back
      val schema = StructType(
        Seq(
          StructField("id", StringType, nullable = true),
          StructField("name", StringType, nullable = true)
        )
      )

      whenReady(s3Connector.readDataS3(testBucketName, "output.csv", schema)) { resultDf =>
        val rows = resultDf.collect()
        rows.length shouldBe 2
        rows(0).getString(0) shouldBe "3"
        rows(0).getString(1) shouldBe "Alice"
        rows(1).getString(0) shouldBe "4"
        rows(1).getString(1) shouldBe "Bob"
      }
    }
  }
}
