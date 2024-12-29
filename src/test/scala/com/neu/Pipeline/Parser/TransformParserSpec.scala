package com.neu.Pipeline.Parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import com.neu.Pipeline.Parser._

class TransformParserSpec extends AnyFlatSpec with Matchers {

  val sampleTransformation = Transformation(
    name = "test_transformation",
    `import` = "./functions.scala",
    inputs = List(
      ("id", "Int"),
      ("name", "String"),
      ("age", "Int")
    ),
    outputs = List(
      OutputFunction("id", "input", "drop", None, None),
      OutputFunction("full_name", "derived", "concatenate", Some(List("name")), Some("String")),
      OutputFunction("age", "input", "validate", None, None)
    )
  )

  val mockCompiledFunctions = new Object()
  val transformParser       = new TransformParser(sampleTransformation, mockCompiledFunctions)

  "TransformParser.inputToStruct" should "create correct StructType from input definitions" in {
    val expectedSchema = StructType(
      Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false)
      )
    )

    val resultSchema = transformParser.inputToStruct

    resultSchema.fields.length should be(3)
    resultSchema should be(expectedSchema)

    // Verify individual fields
    resultSchema.fields(0).name should be("id")
    resultSchema.fields(0).dataType should be(IntegerType)
    resultSchema.fields(0).nullable should be(false)

    resultSchema.fields(1).name should be("name")
    resultSchema.fields(1).dataType should be(StringType)
    resultSchema.fields(1).nullable should be(false)

    resultSchema.fields(2).name should be("age")
    resultSchema.fields(2).dataType should be(IntegerType)
    resultSchema.fields(2).nullable should be(false)
  }

  "TransformParser.outputToStruct" should "transform input schema according to output definitions" in {
    val inputSchema = StructType(
      Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false)
      )
    )

    val expectedSchema = StructType(
      Array(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("full_name", StringType, nullable = false)
      )
    )

    val resultSchema = transformParser.outputToStruct(inputSchema)

    resultSchema.fields.length should be(3)
    resultSchema should be(expectedSchema)

    // Verify that 'id' field is dropped
    resultSchema.fields.map(_.name) should not contain ("id")

    // Verify that 'full_name' field is added
    val fullNameField = resultSchema.fields.find(_.name == "full_name")
    fullNameField should be(defined)
    fullNameField.get.dataType should be(StringType)
    fullNameField.get.nullable should be(false)

    // Verify that 'age' field remains unchanged
    val ageField = resultSchema.fields.find(_.name == "age")
    ageField should be(defined)
    ageField.get.dataType should be(IntegerType)
    ageField.get.nullable should be(false)
  }
}
