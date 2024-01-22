package org.learnSpark.application
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._

object readCSV {

  def readFlight(spark: SparkSession, filePath: String): Dataset[main.flightData] = {
    // Import implicits for converting RDDs to Datasets
    import spark.implicits._

    // Read CSV files into a Dataset[flightData]
    spark.read
      .option("header", "true")
      .schema(main.flightSchema)
      .csv(s"file:$filePath")
      .withColumn("date", to_date(col("date"),"yyyy-MM-dd").cast(DateType))
      .as[main.flightData]
  }

  def readPassenger(spark: SparkSession, filePath: String): Dataset[main.passengers] = {
    // Import implicits for converting RDDs to Datasets
    import spark.implicits._

    // Read CSV files into a Dataset[flightData]
    spark.read
      .option("header", "true")
      .schema(main.passengerSchema)
      .csv(s"file:$filePath")
      .as[main.passengers]
  }

}
