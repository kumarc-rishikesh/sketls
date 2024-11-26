package com.neu.qualitycheck

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataQualityChecks {

  def nullCheck(data: DataFrame, column: String): QualityCheckResult = {
    val failedRows = data.filter(col(column).isNull).count()
    QualityCheckResult(
      checkName = s"Null Check on column: $column",
      passed = failedRows == 0,
      failedRecords = if (failedRows > 0) Some(Seq(s"Rows with nulls: $failedRows")) else None
    )
  }

  def typeCheck(data: DataFrame, column: String, expectedType: String): QualityCheckResult = {
    val incorrectRows = data.filter(!col(column).cast(expectedType).isNotNull).count()
    QualityCheckResult(
      checkName = s"Type Check on column: $column",
      passed = incorrectRows == 0,
      failedRecords = if (incorrectRows > 0) Some(Seq(s"Rows with incorrect type: $incorrectRows")) else None
    )
  }

  def rangeCheck(data: DataFrame, column: String, range: (Double, Double)): QualityCheckResult = {
    val (min, max) = range
    val failedRows = data.filter(col(column).lt(min) || col(column).gt(max)).count()
    QualityCheckResult(
      checkName = s"Range Check on column: $column",
      passed = failedRows == 0,
      failedRecords = if (failedRows > 0) Some(Seq(s"Rows out of range: $failedRows")) else None
    )
  }

  def uniquenessCheck(data: DataFrame, column: String): QualityCheckResult = {
    val duplicateRows = data.groupBy(col(column)).count().filter(col("count") > 1).count()
    QualityCheckResult(
      checkName = s"Uniqueness Check on column: $column",
      passed = duplicateRows == 0,
      failedRecords = if (duplicateRows > 0) Some(Seq(s"Duplicate rows: $duplicateRows")) else None
    )
  }
}

