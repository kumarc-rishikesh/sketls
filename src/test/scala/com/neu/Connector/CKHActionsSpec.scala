package com.neu.Connector

import org.apache.pekko.actor.ActorSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class CKHActionsSpec extends AsyncFlatSpec with Matchers {
  implicit val system: ActorSystem = ActorSystem("CKHActionsTest")
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("CKHActionsTest")
    .getOrCreate()

  val tableName = "ckh_actions_test_table"

  val dropTableQuery   = s"DROP TABLE IF EXISTS $tableName"
  val createTableQuery = s"CREATE TABLE $tableName (column1 String,column2 String) ENGINE = Memory"

  val connector: CKHConnector = CKHConnector()
  val actions: CKHActions     = CKHActions(spark, system, connector)

  Await.result(connector.client.execute(dropTableQuery), 5.minutes)
  Await.result(connector.client.execute(createTableQuery), 5.minutes)

  val schema: StructType = StructType(
    Array(
      StructField("column1", StringType, nullable = true),
      StructField("column2", StringType, nullable = true)
    )
  )

  it should "write and read data from Clickhouse" in {
    val testData = spark
      .createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            ("value1", "value2"),
            ("value3", "value4")
          )
        )
      )
      .toDF("column1", "column2")

    for {
      _        <- actions.writeDataCKH(testData, tableName)
      readData <- actions.readDataCKH(s"SELECT * FROM $tableName", schema)
    } yield {
      readData.count() should be(2)
      readData.columns should contain allOf ("column1", "column2")
    }
  }

  it should "handle empty DataFrame write" in {
    val emptyDF   = spark.emptyDataFrame
    val tableName = "empty_table"

    actions
      .writeDataCKH(emptyDF, tableName)
      .map(_ => succeed)
  }

  it should "handle large batch writes" in {
    val largeData = spark
      .createDataFrame(
        spark.sparkContext.parallelize((1 to 2000).map(i => (s"key$i", s"value$i")))
      )
      .toDF("column1", "column2")

    actions
      .writeDataCKH(largeData, tableName)
      .map(_ => succeed)
  }

  it should "execute select query" in {
    actions
      .readDataCKH("SELECT 1 + 2", schema)
      .map(df => {
        df.count() should be(1)
      })
  }

  def afterAll(): Unit = {
    spark.stop()
    system.terminate()
  }
}
