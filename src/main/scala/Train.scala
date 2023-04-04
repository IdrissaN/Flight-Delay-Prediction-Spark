object Train {

  def main(args: Array[String]): Unit = {

    // create spark session
    val spark = SparkSessionCreator.createSparkSession()

    // train data
    val getData = GetData.getTrain(sparkSession = spark)

    // clean data
    val preprocessData = PreprocessData.cleanData(dataFrame = getData)

    // feature data
    val featureTrainData = FeatureEngineering.featureData(dataFrame = preprocessData)

  }


}
