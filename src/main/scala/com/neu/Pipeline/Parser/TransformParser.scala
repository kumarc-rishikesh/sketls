package com.neu.Pipeline.Parser

import com.neu.Pipeline.Parser.TParser.TransformationFunctionParser
import io.circe.generic.auto._
import io.circe.yaml.parser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

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

class TransformParser (transformation: Transformation, compiledFunctions:Any ) {
  private val outputCols = transformation.outputs

  def inputToStruct :StructType = {
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

  def outputToStruct(ipSchema : StructType): StructType = {
    outputCols.foldLeft(ipSchema)((a:StructType, b:OutputFunction)=> {
      (b.col, b.`type`, b.function, b.datatype)  match {
        case (col, "input", "drop", _) => StructType(a.filterNot(_.name == col ))
        case (_, "input", _, _) => a
        case (col, "derived", _,Some(t)) => StructType(
          a :+ StructField(col, stringToDataType(t), nullable = false)
        )
        case _ => a
      }
    })
  }

  def outputToFunction: (DataFrame => DataFrame) = {
    val functionMap = outputCols.map(b => {
      (b.col, b.`type`, b.function, b.using, b.datatype)  match {
        case (col, "input", "drop", _, _) => (df: DataFrame)=> df.drop(col)
        case (col, "input", f, _, _) => (df: DataFrame) => TransformationFunctionParser
          .getCompiledMethodInputType(compiledFunctions, f)(df, col)
        case (col, "derived", f, Some(usingCols), Some(dt)) => (df: DataFrame) => TransformationFunctionParser
          .getCompiledMethodDerivedType(compiledFunctions, f)(df, usingCols, (col, stringToDataType(dt)))
      }
    })

    functionMap.reduce(_ andThen _)
  }

  private def stringToDataType(dataType: String): DataType = dataType match {
    case "String" => StringType
    case "Int" => IntegerType
    case _ => StringType
  }
}

object TransformParser {
  def apply(filePath: String): Either[String, TransformParser] = {
    try {
      val source = scala.io.Source.fromFile(filePath)
      val yamlString = source.mkString
      source.close()

      parser.parse(yamlString).flatMap(_.as[Transformation]) match {
        case Right(transformation) =>

          val functionSource = scala.io.Source.fromFile(transformation.`import`)
          val functionString = functionSource.mkString
          functionSource.close()

          val compiledFunctions = TransformationFunctionParser.compileFunction(functionString)

          Right(new TransformParser(transformation, compiledFunctions))
        case Left(error) =>
          Left(s"Failed to parse YAML: $error")
      }
    } catch {
      case e: Exception =>
        Left(s"Error reading file: ${e.getMessage}")
    }
  }
}