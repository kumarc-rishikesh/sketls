package com.neu.Qualitycheck

import org.apache.spark.sql.DataFrame
import DataQualityChecks._
import com.neu.Pipeline.Parser.QualityCheck

object QualityCheckRunner {
  private def qualityCheckToMap(qcYaml: List[QualityCheck]): Seq[Map[String, String]] = {
    qcYaml.map { check =>
      val baseMap = Map(
        "type"   -> check.`type`,
        "column" -> check.col
      )

      val optionalFields = Seq(
        check.min.map(v => "min" -> v.toString),
        check.max.map(v => "max" -> v.toString),
        check.expected_type.map(v => "expected_type" -> v)
      ).flatten.toMap

      baseMap ++ optionalFields
    }
  }

  def runChecks(data: DataFrame, checks_ : List[QualityCheck]): Seq[QualityCheckResult] = {
    val checks           = qualityCheckToMap(checks_)
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
          case "null_check"       => nullCheck(data, column)
          case "type_check"       => typeCheck(data, column, check("expected_type"))
          case "range_check"      =>
            val range = (check("min").toDouble, check("max").toDouble)
            rangeCheck(data, column, range)
          case "uniqueness_check" => uniquenessCheck(data, column)
          case _                  =>
            throw new IllegalArgumentException(s"Unknown check type: ${check("type")}")
        }
      }
    }
  }

  def getResultCount(qcResults: Seq[QualityCheckResult]): Int = {
    qcResults.foldLeft(0) { (acc, result) =>
      result match {
        case r if r.passed => acc + 1
        case r             =>
          r.failedRecords.foreach(records => println(s"Failed Records: ${records.mkString("\n")}"))
          acc
      }
    }
  }
}
