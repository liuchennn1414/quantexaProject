package org.learnSpark.application
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._

import java.net.URL
import scala.io.Source
import java.nio.file.{Paths, Path}

object ReadCSV {

  def readFlight(spark: SparkSession, filePath: String): Dataset[Main.flightData] = {
    // Import implicits for converting RDDs to Datasets
    import spark.implicits._

    val resource: Path = Paths.get(filePath).toAbsolutePath
    val absolutePath: String = resource.toString

    // Read CSV files for flightData
    spark.read
      .option("header", "true")
      .schema(Main.flightSchema)
      .csv(s"file:$absolutePath")
      .withColumn("date", to_date(col("date"),"yyyy-MM-dd").cast(DateType)) // convert date string to Date type
      .as[Main.flightData]
  }

  def readPassenger(spark: SparkSession, filePath: String): Dataset[Main.passengers] = {
    // Import implicits for converting RDDs to Datasets
    import spark.implicits._

    val resource: Path = Paths.get(filePath).toAbsolutePath
    val absolutePath: String = resource.toString

    // Read CSV files for passengers
    spark.read
      .option("header", "true")
      .schema(Main.passengerSchema)
      .csv(s"file:$absolutePath")
      .as[Main.passengers]
  }

}
