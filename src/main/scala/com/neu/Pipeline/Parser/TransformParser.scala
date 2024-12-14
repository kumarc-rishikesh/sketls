package com.neu.Pipeline.Parser

import io.circe.generic.auto._
import io.circe.yaml.parser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.util.Try

case class Transformation(
    name: String,
    `import`: String,
    inputs: List[(String, String)],
    outputs: List[OutputFunction]
)

case class OutputFunction(
    col: String,
    `type`: String,
    function: String,
    using: Option[List[String]],
    datatype: Option[String]
)

class TransformParser(transformation: Transformation, compiledFunctions: Any) {
  private val outputCols = transformation.outputs

  def inputToStruct: StructType = {
    StructType(
      transformation.inputs.map { case (name, dataType) =>
        StructField(
          name,
          stringToDataType(dataType),
          nullable = false
        )
      }
    )
  }

  def outputToStruct(ipSchema: StructType): StructType = {
    outputCols.foldLeft(ipSchema)((a: StructType, b: OutputFunction) => {
      (b.col, b.`type`, b.function, b.datatype) match {
        case (col, "input", "drop", _)    => StructType(a.filterNot(_.name == col))
        case (_, "input", _, _)           => a
        case (col, "derived", _, Some(t)) =>
          StructType(
            a :+ StructField(col, stringToDataType(t), nullable = false)
          )
        case _                            => a
      }
    })
  }

  def outputToFunction: (DataFrame => DataFrame) = {
    val functionMap = outputCols.map(b => {
      (b.col, b.`type`, b.function, b.using, b.datatype) match {
        case (col, "input", "drop", _, _)                   => (df: DataFrame) => df.drop(col)
        case (col, "input", f, _, _)                        =>
          (df: DataFrame) =>
            TransformationFunctionParser
              .getCompiledMethodInputType(compiledFunctions, f)(df, col)
        case (col, "derived", f, Some(usingCols), Some(dt)) =>
          (df: DataFrame) =>
            TransformationFunctionParser
              .getCompiledMethodDerivedType(compiledFunctions, f)(df, usingCols, (col, stringToDataType(dt)))
      }
    })

    functionMap.reduce(_ andThen _)
  }

  private def stringToDataType(dataType: String): DataType = dataType match {
    case "String" => StringType
    case "Int"    => IntegerType
    case _        => StringType
  }
}

object TransformParser {
  def apply(filePath: String): Try[TransformParser] = {
    def readFile(path: String): Try[String] = {
      Try {
        val source = scala.io.Source.fromFile(path)
        try {
          source.mkString
        } finally {
          source.close()
        }
      }
    }

    for {
      // Read and parse the main YAML file
      yamlString        <- readFile(filePath)
      transformation    <- Try(
                             parser.parse(yamlString).flatMap(_.as[Transformation]) match {
                               case Right(t)    => t
                               case Left(error) => throw new RuntimeException(s"Failed to parse YAML: $error")
                             }
                           )

      // Read and compile the function file
      functionString    <- readFile(transformation.`import`)
      compiledFunctions <- Try(TransformationFunctionParser.compileFunction(functionString))

      // Create the TransformParser instance
      transformParser <- Try(new TransformParser(transformation, compiledFunctions))
    } yield transformParser
  }
}
