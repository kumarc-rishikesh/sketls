import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Functions {
  def toLower(df: DataFrame, ipFieldInfo: String): DataFrame = {
    df.withColumn(ipFieldInfo, lower(col(ipFieldInfo)))
  }

  def toUpper(df: DataFrame, ipFieldInfo: String): DataFrame = {
    df.withColumn(ipFieldInfo, upper(col(ipFieldInfo)))
  }

  def toDateYYYYMM(df: DataFrame, ipFieldInfo: Seq[String], opFieldInfo: (String, DataType)): DataFrame = {
    val yearCol = ipFieldInfo(0)
    val monthCol = ipFieldInfo(1)
    val opFieldName = opFieldInfo._1
    val opFieldType = opFieldInfo._2
    df.withColumn(opFieldName,
      concat_ws("-", col(yearCol).cast(opFieldType), col(monthCol).cast(opFieldType)))
  }
}

Functions

