import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Functions {
  def toUpper(df: DataFrame, ipFieldInfo: String): DataFrame = {
    df.withColumn(ipFieldInfo, upper(col(ipFieldInfo)))
  }
}
Functions
