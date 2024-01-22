package org.learnSpark.application
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf


object Question3 {
  import org.apache.spark.sql.Encoders
  case class PassengerRoute(passengerId: Int, routes: Seq[String])
  case class PassengerNonUKRouteCount(passengerId: Int, longestRun: Int)

  def getLongestNonUKChain(route:Seq[String]): Int = {
    // User Defined Function to count consecutive cities without visiting UK
    val (_, maxLength, currentLength) = route.foldLeft((false,0,0)) {
      case((inChain, maxLength, currentLength), city) =>
        if (city != "uk") {
          (true, maxLength, currentLength + 1)
        }else{
          (false, Math.max(maxLength, currentLength),0)
        }
    }
    maxLength
  }

  def getPassengerRoute(spark: SparkSession, flightData: Dataset[main.flightData]): Dataset[PassengerRoute] = {
    import spark.implicits._
    val passengerRoute = flightData
      .orderBy("passengerId","date")
      .groupBy("passengerId")
      .agg(
        first("from").as("firstFrom"),
        collect_list("to").as("routes"))
      .withColumn("routes",concat(array($"firstFrom"), $"routes").as("routes"))
      .drop("firstFrom")
      .as[PassengerRoute]
    passengerRoute
  }

  def output(spark: SparkSession, flightData: Dataset[main.flightData]): Dataset[PassengerNonUKRouteCount] = {
    val routeDS = getPassengerRoute(spark, flightData)
    val countRun = udf(getLongestNonUKChain _)
    val outputDS = routeDS
      .withColumn("longestRun", countRun(col("routes")))
      .orderBy(col("longestRun").desc)
      .select("passengerId","longestRun")
      .as[PassengerNonUKRouteCount](Encoders.product[PassengerNonUKRouteCount])
    outputDS
  }

}

