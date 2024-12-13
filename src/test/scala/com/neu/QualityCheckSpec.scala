package com.neu

import com.neu.qualitycheck.DataQualityChecks
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite


class QualityCheckSpec extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("Quality Check Tests")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Helper function to create a DataFrame from a sequence
  def createDataFrame(data: Seq[Map[String, Any]]): DataFrame = {
    val columns = data.head.keys.toSeq
    val rows = data.map(_.values.toSeq)
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows.map(Row.fromSeq)),
      StructType(columns.map(StructField(_, StringType, nullable = true)))
    )
  }

  test("Null Check - Passes when no nulls exist") {
    val data = Seq(
      Map("borough" -> "A", "year" -> "2020", "values" -> "100", "lsoa_code" -> "LSOA1"),
      Map("borough" -> "B", "year" -> "2021", "values" -> "200", "lsoa_code" -> "LSOA2")
    )
    val df = createDataFrame(data)
    val result = DataQualityChecks.nullCheck(df, "borough")
    assert(result.passed)
    assert(result.failedRecords.isEmpty)
  }

  test("Null Check - Fails when nulls exist") {
    val data = Seq(
      Map("borough" -> null, "year" -> "2020", "values" -> "100", "lsoa_code" -> "LSOA1"),
      Map("borough" -> "B", "year" -> "2021", "values" -> "200", "lsoa_code" -> "LSOA2")
    )
    val df = createDataFrame(data)
    val result = DataQualityChecks.nullCheck(df, "borough")
    assert(!result.passed)
    assert(result.failedRecords.nonEmpty)
  }

  test("Type Check - Passes for correct types") {
    val data = Seq(
      Map("borough" -> "A", "year" -> "2020", "values" -> "100", "lsoa_code" -> "LSOA1"),
      Map("borough" -> "B", "year" -> "2021", "values" -> "200", "lsoa_code" -> "LSOA2")
    )
    val df = createDataFrame(data)
    val result = DataQualityChecks.typeCheck(df, "year", "int")
    assert(result.passed)
    assert(result.failedRecords.isEmpty)
  }

  test("Type Check - Fails for incorrect types") {
    val data = Seq(
      Map("borough" -> "A", "year" -> "2020", "values" -> "100", "lsoa_code" -> "LSOA1"),
      Map("borough" -> "B", "year" -> "abc", "values" -> "200", "lsoa_code" -> "LSOA2")
    )
    val df = createDataFrame(data)
    val result = DataQualityChecks.typeCheck(df, "year", "int")
    assert(!result.passed)
    assert(result.failedRecords.nonEmpty)
  }

  test("Range Check - Passes when values are in range") {
    val data = Seq(
      Map("borough" -> "A", "year" -> "2020", "values" -> "50", "lsoa_code" -> "LSOA1"),
      Map("borough" -> "B", "year" -> "2021", "values" -> "200", "lsoa_code" -> "LSOA2")
    )
    val df = createDataFrame(data)
    val result = DataQualityChecks.rangeCheck(df, "values", (0, 300))
    assert(result.passed)
    assert(result.failedRecords.isEmpty)
  }

  test("Range Check - Fails when values are out of range") {
    val data = Seq(
      Map("borough" -> "A", "year" -> "2020", "values" -> "400", "lsoa_code" -> "LSOA1"),
      Map("borough" -> "B", "year" -> "2021", "values" -> "200", "lsoa_code" -> "LSOA2")
    )
    val df = createDataFrame(data)
    val result = DataQualityChecks.rangeCheck(df, "values", (0, 300))
    assert(!result.passed)
    assert(result.failedRecords.nonEmpty)
  }

  test("Uniqueness Check - Passes when column is unique") {
    val data = Seq(
      Map("borough" -> "A", "year" -> "2020", "values" -> "100", "lsoa_code" -> "LSOA1"),
      Map("borough" -> "B", "year" -> "2021", "values" -> "200", "lsoa_code" -> "LSOA2")
    )
    val df = createDataFrame(data)
    val result = DataQualityChecks.uniquenessCheck(df, "lsoa_code")
    assert(result.passed)
    assert(result.failedRecords.isEmpty)
  }

  test("Uniqueness Check - Fails when duplicates exist") {
    val data = Seq(
      Map("borough" -> "A", "year" -> "2020", "values" -> "100", "lsoa_code" -> "LSOA1"),
      Map("borough" -> "B", "year" -> "2021", "values" -> "200", "lsoa_code" -> "LSOA1")
    )
    val df = createDataFrame(data)
    val result = DataQualityChecks.uniquenessCheck(df, "lsoa_code")
    assert(!result.passed)
    assert(result.failedRecords.nonEmpty)
  }
}
