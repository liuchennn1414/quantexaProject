package org.learnSpark.application
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._

import scala.io.Source
import java.nio.file.{Paths, Path}

object ReadCSV {

  import Main.spark.implicits._

  def readFlight(filePath: String): Dataset[Main.flightData] = {

    // to retrieve absolute path
    val resource: Path = Paths.get(filePath).toAbsolutePath
    val absolutePath: String = resource.toString

    // Read CSV files for flightData
    Main.spark.read
      .option("header", "true")
      .schema(Main.flightSchema)
      .csv(s"file:$absolutePath")
      .withColumn("date", to_date(col("date"),"yyyy-MM-dd").cast(DateType)) // convert date string to Date type
      .as[Main.flightData]
  }

  def readPassenger(filePath: String): Dataset[Main.passengers] = {

    val resource: Path = Paths.get(filePath).toAbsolutePath
    val absolutePath: String = resource.toString

    // Read CSV files for passengers
    Main.spark.read
      .option("header", "true")
      .schema(Main.passengerSchema)
      .csv(s"file:$absolutePath")
      .as[Main.passengers]
  }

}
