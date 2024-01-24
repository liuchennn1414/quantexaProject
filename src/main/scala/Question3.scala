package org.learnSpark.application

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

import scala.annotation.tailrec

object Question3 extends Serializable {
  @transient
  // created 2 new case classes which will be commonly used below
  case class passengerRoute(passengerId: Int, routes: List[String])
  case class passengerNonUKRouteCount(passengerId: Int, longestRun: Int)

  // import spark
  import Main.spark.implicits._

  // helper function 1: get the distinct count of country without visiting uk
  def getLongestNonUkRun(route: Seq[String]): Int = {
    val (maxLength, _, _) = route.foldLeft((0, 0, Set.empty[String])) { // iterate over the full routes
      case ((maxLength, currentLength, visitedSet), country) =>
        if (country.toLowerCase() != "uk") { // if the country is not UK
          if (visitedSet.contains(country.toLowerCase())) { // and if the country has been visited before within this run
            (maxLength, currentLength, visitedSet) // do not update anything
          } else { // if the country is not UK and has not been visited before within this run
            (Math.max(maxLength, currentLength + 1), currentLength + 1, visitedSet + country.toLowerCase()) // update the max length and current length
          }
        } else {
          (Math.max(maxLength, currentLength), 0, Set.empty[String]) // If country is UK, clean up the current length and the visited list; a new run will be started
        }
    }
    maxLength
  }

  // helper function 2: given 2 list from and to, find the full routes of passenger (include domestic flight)
  @tailrec
  def createRoute(from: List[String], to: List[String], routes: List[String]): List[String] = {
    (from, to) match { // pattern matching, as from and to list are not null:
      case (fromHead :: fromTail, toHead :: toTail) => // break each list to [0] and [1:]
        if (routes.isEmpty || routes.last != fromHead) { // if list is empty, or the previous to != current front
          createRoute(fromTail, toTail, routes :+ fromHead :+ toHead) // append current from -> to into the route
        } else {
          createRoute(fromTail, toTail, routes :+ toHead) // else if previous to == current from, only append the next destination to avoid double count
        }

      case _ => routes // output list after iteration is done
    }
  }

  // wrap up the 2 functions with udf (user defined function) so as to use them in spark
  val createRouteUDF: UserDefinedFunction = udf((from: Seq[String], to: Seq[String]) => {
    createRoute(from.toList, to.toList, List.empty[String])
  }: List[String]) // collect_list returns a Seq

  val countRun: UserDefinedFunction = udf(getLongestNonUkRun _)

  // get passenger routes and output as [passengerId, routes]
  def getPassengerRoute(flightData: Dataset[Main.flightData]): Dataset[passengerRoute] = {

    val passengerRoute = flightData
      .orderBy("passengerId", "date", "flightId") // if there are more than one flight on one day, assume it follows flightId order
      .groupBy("passengerId")
      .agg(createRouteUDF(collect_list("from"), collect_list("to")).as("routes")) //collect_list returns Seq instead of List
      .as[passengerRoute]

    passengerRoute
  }

  // output function to output the longest run for each passenger
  def output(flightData: Dataset[Main.flightData]): Dataset[passengerNonUKRouteCount] = {

    val routeDS = getPassengerRoute(flightData)
    val output = routeDS
      .withColumn("longestRun", countRun(col("routes")))
      .orderBy(col("longestRun").desc,col("passengerId"))
      .select("passengerId", "longestRun")
      .as[passengerNonUKRouteCount]
    output
  }
}
