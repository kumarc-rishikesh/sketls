package com.neu

import com.neu.qualitycheck.QualityCheckRunner
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Quality Checks Example")
      .master("local[*]")
      .getOrCreate()

    val data = spark.read.option("header", "true").csv("london_crime_by_lsoa_sample.csv")

    // Example YAML-like Quality Check Config
    val qualityCheckConfig = Seq(
      Map("type" -> "null_check", "column" -> "borough"),
      Map("type" -> "type_check", "column" -> "year", "expected_type" -> "int"),
      Map("type" -> "range_check", "column" -> "values", "min" -> "0", "max" -> "1000"),
      Map("type" -> "uniqueness_check", "column" -> "lsoa_code")
    )

    // Run Quality Checks
    val results = QualityCheckRunner.runChecks(data, qualityCheckConfig)

    // Display Results
    results.foreach { result =>
      println(s"Check: ${result.checkName}, Result of check: ${result.passed}")
      result.failedRecords.foreach { records =>
        println(s"Failed Records: ${records.mkString("\n")}")
      }
    }

    spark.stop()
  }
}
