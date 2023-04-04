import org.apache.spark.sql.SparkSession

object SparkSessionCreator {

  def createSparkSession(): SparkSession = {

    SparkSession
      .builder()
      .master("local[*]")
      .appName("scala-flight-delay-project")
      .getOrCreate()

  }

}
