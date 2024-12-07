package com.neu

import Parsers.ConfigParser

object Main {
  def main(args: Array[String]): Unit = {
    val configSource     = scala.io.Source.fromFile("docs/sample_config.yaml")
    val configYAMLString = configSource.mkString
    configSource.close()
    ConfigParser.parsePipelineConfig(configYAMLString) match {
      case Right(pipeline) => println(s"Pipeline parsed successfully: ${pipeline}")
      case Left(error)     => println(s"Failed to parse pipeline: ${error.getMessage}")
    }

  }
}
