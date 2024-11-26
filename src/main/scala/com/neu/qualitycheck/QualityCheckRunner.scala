package com.neu.qualitycheck

import org.apache.spark.sql.DataFrame

object QualityCheckRunner {
  import DataQualityChecks._

  def runChecks(data: DataFrame, checks: Seq[Map[String, String]]): Seq[QualityCheckResult] = {
    // Get the list of columns in the DataFrame
    val availableColumns = data.columns.toSet

    // Process each check
    checks.map { check =>
      // Extract the column name for the current check
      val column = check.getOrElse("column", "")

      // Validate if the column exists
      if (!availableColumns.contains(column)) {
        QualityCheckResult(
          checkName = s"Column Existence Check for: $column",
          passed = false,
          failedRecords = Some(Seq(s"Column '$column' not found in source"))
        )
      } else {
        // Perform the quality check based on its type
        check("type") match {
          case "null_check" => nullCheck(data, column)
          case "type_check" => typeCheck(data, column, check("expected_type"))
          case "range_check" =>
            val range = (check("min").toDouble, check("max").toDouble)
            rangeCheck(data, column, range)
          case "uniqueness_check" => uniquenessCheck(data, column)
          case _ =>
            throw new IllegalArgumentException(s"Unknown check type: ${check("type")}")
        }
      }
    }
  }
}

