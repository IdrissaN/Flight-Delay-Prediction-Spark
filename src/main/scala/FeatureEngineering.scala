import org.apache.spark.ml.feature.{Bucketizer, StringIndexer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.when


object FeatureEngineering {

  def featureData(dataFrame: DataFrame): DataFrame = {

    // function to index delay col
    def delayIndexer(dataFrame: DataFrame): DataFrame = {

      val indexer = new StringIndexer()
        .setInputCol("dep_delayed_15min")
        .setOutputCol("Label")

      val indexed = indexer.fit(dataFrame).transform(dataFrame)

      indexed

    }

    def distanceBucketizer(dataFrame: DataFrame): DataFrame = {

      val distanceBins = Array(0.0, 500.0, 1000.0, 1500.0, 2000.0, 2500.0)

      val distanceBucketizer = new Bucketizer()
        .setInputCol("Distance")
        .setOutputCol("DistanceBin")
        .setSplits(distanceBins)

      // add distance levels to input data frame
      distanceBucketizer.transform(dataFrame)

    }

    def depTimeBin(col: Column): Column = {
      when(col < 600, "Night")
        .when(col >= 600 && col <= 1200, "Morning")
        .when(col >= 1200 && col <= 1800, "Afternoon")
        .when(col >= 1800 && col <= 2600, "Evening")

    }

    def addSeason(col: Column): Column = {

        when(col >= 6 && col <= 8, "summer")
        .when(col >= 9 && col <= 11, "autumn")
        .when(col >= 3 && col <= 5, "spring")
        .otherwise("winter")

    }

    def addTimeFeatures(dataFrame: DataFrame): DataFrame = {

      dataFrame
        .withColumn("DepTime", depTimeBin(dataFrame("DepTime"))).as("depTimeBin")
        .withColumn("Month", addSeason(dataFrame("Month"))).as("season")
    }

    val indexData = delayIndexer(dataFrame)
    val bucketData = distanceBucketizer(indexData)
    val extendData = addTimeFeatures(bucketData)

    extendData

  }

}

