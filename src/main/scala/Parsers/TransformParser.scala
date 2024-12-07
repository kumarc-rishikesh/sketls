package Parsers

import io.circe.yaml.parser
import io.circe.generic.auto._
import org.apache.spark.sql.types._

object YamlParser extends App {
  case class Transformation(
      name: String,
      imports: List[String],
      inputs: List[(String, String)]
  )

  val transformSource     = scala.io.Source.fromFile("docs/sample_transformation_config.yaml")
  val transformYAMLString = transformSource.mkString
  transformSource.close()

  val result = parser.parse(transformYAMLString).flatMap(_.as[Transformation])

  result match {
    case Right(transformation) => {
      val schema = StructType(
        transformation.inputs.map { case (name, dataType) =>
          StructField(
            name,
            dataType match {
              case "String" => StringType
              case "Int"    => IntegerType
            },
            nullable = false
          )
        }
      )
      println(schema)
    }
    case Left(error)           => println(s"Failed to parse: $error")
  }

}
