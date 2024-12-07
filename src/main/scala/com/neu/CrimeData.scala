package com.neu


import org.apache.spark.sql.types._

object CrimeData {
  val schema = StructType(Array(
    StructField("lsoa_code", StringType, nullable = false),
    StructField("borough", StringType, nullable = false),
    StructField("major_category", StringType, nullable = false),
    StructField("minor_category", StringType, nullable = false),
    StructField("value", IntegerType, nullable = false),
    StructField("year", IntegerType, nullable = false),
    StructField("month", IntegerType, nullable = false)
  ))
}
