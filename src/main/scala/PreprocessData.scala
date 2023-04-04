import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object PreprocessData {

  def cleanData(dataFrame: DataFrame): DataFrame = {

    def formatData(dataFrame: DataFrame): DataFrame = {

      dataFrame
        .withColumn("Distance", dataFrame("Distance").cast(DoubleType))
        .withColumn("DepTime", dataFrame("DepTime").cast(DoubleType))
        .withColumn("DayOfWeek", split(dataFrame("DayOfWeek"), "-")(1).cast(IntegerType)).as("DayOfWeek")
        .withColumn("DayOfMonth", split(dataFrame("DayOfMonth"), "-")(1).cast(IntegerType)).as("DayOfMonth")
        .withColumn("Month", split(dataFrame("DayOfMonth"), "-")(1).cast(IntegerType)).as("Month")
    }

    // replace missing distance values by mean
    def meanDistance(dataFrame: DataFrame): Double = {

      dataFrame
        .select("Distance")
        .na.drop()
        .agg(round(mean("Distance"), 2))
        .first()
        .getDouble(0)
    }

    val preprocessData = formatData(dataFrame)
    preprocessData

  }

}
