package com.neu.Pipeline.Parser

import com.neu.Pipeline.Parser.TransformationFunctionParser
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TransformationFunctionParserSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("TransformationFunctionParserTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }
  test("toLower function compilation and execution") {
    // Create test data
    val testData = spark
      .createDataFrame(
        Seq(
          ("ID1", "HELLO"),
          ("ID2", "WORLD")
        )
      )
      .toDF("id", "text")

    val functionSource = """
    import org.apache.spark.sql.DataFrame
    import org.apache.spark.sql.functions._

    object Functions {
      def toLower(df: DataFrame, columnName: String): DataFrame = {
        df.withColumn(columnName, lower(col(columnName)))
      }
    }

    Functions
  """

    val functions = TransformationFunctionParser.compileFunction(functionSource)

    val toLowerFn = TransformationFunctionParser.getCompiledMethodInputType(functions, "toLower")

    val result = toLowerFn(testData, "text")

    val expected = spark
      .createDataFrame(
        Seq(
          ("ID1", "hello"),
          ("ID2", "world")
        )
      )
      .toDF("id", "text")

    assert(result.collect().toSet == expected.collect().toSet)
  }

  test("dateYYYYMMStr function compilation and execution") {
    // Create test data
    val testData = spark
      .createDataFrame(
        Seq(
          ("E01001116", 2016, 11),
          ("E01001646", 2015, 5)
        )
      )
      .toDF("lsoa_code", "year", "month")

    // Source code of the function we want to compile
    val functionSource = """
      import org.apache.spark.sql.DataFrame
      import org.apache.spark.sql.functions._
      import org.apache.spark.sql.types._

      object Functions {
        def dateYYYYMMStr(df: DataFrame, ipFieldInfo: Seq[String], opFieldInfo: (String, DataType)): DataFrame = {
          val yearCol = ipFieldInfo(0)
          val monthCol = ipFieldInfo(1)
          val opFieldName = opFieldInfo._1
          val opFieldType = opFieldInfo._2
          df.withColumn(opFieldName,
            concat_ws("-", col(yearCol).cast(opFieldType), col(monthCol).cast(opFieldType)))
        }
      }

      Functions
    """

    // Compile and get the Functions object
    val functions = TransformationFunctionParser.compileFunction(functionSource)

    // Get the compiled method
    val dateYYYYMMFn = TransformationFunctionParser.getCompiledMethodDerivedType(
      functions,
      "dateYYYYMMStr"
    )

    // Execute the function
    val result = dateYYYYMMFn(
      testData,
      Seq("year", "month"),
      ("year_month", StringType)
    )

    // Create expected DataFrame
    val expected = spark
      .createDataFrame(
        Seq(
          ("E01001116", 2016, 11, "2016-11"),
          ("E01001646", 2015, 5, "2015-5")
        )
      )
      .toDF("lsoa_code", "year", "month", "year_month")

    // Compare DataFrames
    assert(result.collect().toSet == expected.collect().toSet)
  }
}
