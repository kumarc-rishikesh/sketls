package com.neu.Pipeline.Parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.neu.Pipeline.Parser._

class ConfigParserSpec extends AnyFlatSpec with Matchers {

  val yamlString = """
                     |pipeline:
                     |  name: "example_pipeline_1"
                     |  jobs:
                     |    - jobname: "example_job_1"
                     |      trigger:
                     |          type: "cron"
                     |          value: "0 1 * * *"
                     |      source:
                     |          type: "clickhouse"
                     |          table: "crime_data"
                     |          query: "SELECT * FROM crime_data limit 50 FORMAT CSV"
                     |      transformation:
                     |          definition: "./docs/sample_transformation_config.yaml"
                     |      quality_checks:
                     |        - type: "null_check"
                     |          col: "borough"
                     |        - type: "type_check"
                     |          col: "year"
                     |          expected_type: "int"
                     |        - type: "range_check"
                     |          col: "value"
                     |          min: 0
                     |          max: 1000
                     |      destination:
                     |          type: "s3"
                     |          bucket: "test"
                     |          filename: "xyz.csv"
                     |
                     |    - jobname: "example_job_2"
                     |      trigger:
                     |          type: "cron"
                     |          value: "10 1 * * *"
                     |      source:
                     |          type: "s3"
                     |          bucket: "test"
                     |          filename: "xyz.csv"
                     |      transformation:
                     |          definition: "./docs/sample_transformation_config2.yaml"
                     |      quality_checks:
                     |        - type: "type_check"
                     |          col: "major_category"
                     |          expected_type: "string"
                     |      destination:
                     |          type: "clickhouse"
                     |          table: "crime_data2"
    """.stripMargin

  "ConfigParser" should "parse the pipeline configuration correctly" in {
    val result = ConfigParser.parsePipelineConfig(yamlString)
    result.isSuccess should be(true)

    val pipeline = result.get
    pipeline.pipeline.name should be(Some("example_pipeline_1"))
    pipeline.pipeline.jobs should have size 2

    val job1 = pipeline.pipeline.jobs.head
    job1.jobname should be("example_job_1")
    job1.trigger.`type` should be("cron")
    job1.trigger.value should be("0 1 * * *")
    job1.source.`type` should be("clickhouse")
    job1.source.table should be(Some("crime_data"))
    job1.source.query should be(Some("SELECT * FROM crime_data limit 50 FORMAT CSV"))
    job1.transformation.definition should be("./docs/sample_transformation_config.yaml")
    job1.quality_checks should have size 3
    job1.destination.`type` should be("s3")
    job1.destination.bucket should be(Some("test"))
    job1.destination.filename should be(Some("xyz.csv"))

    val job2 = pipeline.pipeline.jobs(1)
    job2.jobname should be("example_job_2")
    job2.trigger.`type` should be("cron")
    job2.trigger.value should be("10 1 * * *")
    job2.source.`type` should be("s3")
    job2.source.bucket should be(Some("test"))
    job2.source.filename should be(Some("xyz.csv"))
    job2.transformation.definition should be("./docs/sample_transformation_config2.yaml")
    job2.quality_checks should have size 1
    job2.destination.`type` should be("clickhouse")
    job2.destination.table should be(Some("crime_data2"))
  }

  it should "parse quality checks correctly" in {
    val result = ConfigParser.parsePipelineConfig(yamlString)
    result.isSuccess should be(true)

    val pipeline = result.get
    val job1 = pipeline.pipeline.jobs.head

    job1.quality_checks should have size 3

    val nullCheck = job1.quality_checks.find(_.`type` == "null_check")
    nullCheck should be(defined)
    nullCheck.get.col should be("borough")

    val typeCheck = job1.quality_checks.find(_.`type` == "type_check")
    typeCheck should be(defined)
    typeCheck.get.col should be("year")
    typeCheck.get.expected_type should be(Some("int"))

    val rangeCheck = job1.quality_checks.find(_.`type` == "range_check")
    rangeCheck should be(defined)
    rangeCheck.get.col should be("value")
    rangeCheck.get.min should be(Some(0))
    rangeCheck.get.max should be(Some(1000))
  }
}
