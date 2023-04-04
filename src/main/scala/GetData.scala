import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object GetData {

  def getTrain(sparkSession: SparkSession): DataFrame = {
    // load train data from local
    sparkSession.read.option("header", "true").csv("./src/main/resources/flight_delays_train.csv")
  }

  def getTest(sparkSession: SparkSession): DataFrame = {
    // load test data from local
    sparkSession.read.option("header", "true").csv("./src/main/resources/flight_delays_test.csv")

  }

}