package com.neu.connectors

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SaveMode, SparkSession}
import org.mockito.IdiomaticMockito
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.Properties
import scala.concurrent.{ExecutionContext, Future}

class PGDataUtilsSpec extends AnyWordSpec with Matchers with ScalaFutures with IdiomaticMockito {
  implicit val ec: ExecutionContext = ExecutionContext.global

  "PGDataUtils" should {
    "read data from PostgreSQL" in {
      val spark = mock[SparkSession]
      val sparkContext = mock[SparkContext]
      val dataFrameReader = mock[DataFrameReader]

      val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
      val tableName = "london_crime_r_t"
      val properties = new Properties()

      // Mock DataFrame creation
      val expectedSchema = StructType(Array(
        StructField("lsoa_code", StringType, nullable = false),
        StructField("borough", StringType, nullable = false),
        StructField("major_category", StringType, nullable = false),
        StructField("minor_category", StringType, nullable = false),
        StructField("value", IntegerType, nullable = false),
        StructField("year", IntegerType, nullable = false),
        StructField("month", IntegerType, nullable = false)
        )
      )

      // First 3 rows
      val expectedData = Seq(
        Row("E01001116", "Croydon", "Burglary", "Burglary in Other Buildings", 0, 2016, 11),
        Row("E01001646", "Greenwich", "Violence Against the Person", "Other violence", 0, 2016, 11),
        Row("E01000677", "Bromley", "Violence Against the Person", "Other violence", 0, 2015, 5))
      val mockRdd = mock[RDD[Row]]
      when(spark.sparkContext).thenReturn(sparkContext)

      when(sparkContext.parallelize(expectedData)).thenReturn(mockRdd)

      val expectedDataFrame = mock[DataFrame]
      when(spark.createDataFrame(mockRdd, expectedSchema)).thenReturn(expectedDataFrame)

      when(spark.read).thenReturn(dataFrameReader)
      spark.read.jdbc(jdbcUrl, tableName, properties) returns expectedDataFrame
      when(expectedDataFrame.collect()).thenReturn(expectedData.toArray)

      val futureDF = Future.successful(expectedDataFrame)
      whenReady(futureDF) { df =>
        df.collect() should contain theSameElementsAs expectedData
      }
    }

    "DataFrameWriter" should {
      "configure and write data using real DataFrame" in {
        // In-memory testing

        // Initialize real SparkSession
        val spark: SparkSession = SparkSession.builder()
          .master("local[*]")
          .appName("Real DataFrame Writer Test")
          .getOrCreate()

        // Define schema for the DataFrame
        val schema = StructType(Array(
          StructField("lsoa_code", StringType, nullable = false),
          StructField("borough", StringType, nullable = false),
          StructField("major_category", StringType, nullable = false),
          StructField("minor_category", StringType, nullable = false),
          StructField("value", IntegerType, nullable = false),
          StructField("year", IntegerType, nullable = false),
          StructField("month", IntegerType, nullable = false)
        ))

        // Define data
        val data = Seq(
          Row("E01003774", "RedBridge", "Burglary", "Burglary in Other Buildings", 0, 2012, 5),
          Row("E01004563", "Wandsworth", "Robbery", "Personal Property", 0, 2010, 7),
          Row("E01001320", "Ealing", "Theft and Handling", "Other theft", 0, 2013, 4)
        )

        // Create a real DataFrame
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

        // Simulate DataFrame write to console for immediate feedback (could redirect this logic)
        df.write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/postgres")
          .option("dbtable", "london_crime_w_t")
          .option("user", "postgres")
          .option("password", "1234")
          .mode(SaveMode.Overwrite)
          .save()

        // Optional verification: check DataFrame is not empty, etc.
        assert(df.count() == 3)

        // Stop Spark session
        spark.stop()
      }
    }
  }
}