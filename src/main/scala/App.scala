package ru.otus.spark
import org.postgresql.Driver
import org.apache.spark.sql.functions.{stddev, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SaveMode}
import org.apache.spark.rdd.RDD._


object App extends SparkSessionWrapper {
  import spark.implicits._

  case class ridesData(trip_distance: Double)

  def main(args : Array[String]) = {

    // --------------------------------------------------------------
    // Задание 1
    // Построить таблицу, которая покажет какие районы самые популярные для заказов

    // Подгружаем фактические данные поездок
    val ridesFactData: DataFrame = spark.read.format("parquet")
          .option("mode", "FAILFAST")
          .load("src/main/resources/data/yellow_taxi_jan_25_2018")

    // подгружаем справочные данные поездок
    val ridesRefData: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .load("src/main/resources/data/taxi_zones.csv")

    val TopDistrictsByRidesStat:DataFrame = ridesFactData
      .select(col("PULocationID").alias("LocationID"))
      .join(
        ridesRefData.select(
          "LocationID",
          "Borough"
        ),
        "LocationID"
      )
      .groupBy("Borough")
      .agg(count("LocationID").alias("RidesCount"))
      .orderBy(desc("RidesCount"))

    TopDistrictsByRidesStat
      .limit(10)
      .show

    TopDistrictsByRidesStat
      .coalesce((1))
      .write
      .parquet("src/main/resources/data/Res1")

    // --------------------------------------------------------------

    // --------------------------------------------------------------
    // Задание 2
    // Построить таблицу, которая покажет в какое время происходит больше всего вызовов
    val ridesFactDataRdd: RDD[Row] = ridesFactData.rdd

    // Для каждого заказа смотрим, в какой час был сделан вызов
    // группируем по часу
    // сортируем по количеству заказов в этот час
    val TopHoursForRide: RDD[(Int, Int)] = ridesFactDataRdd
      .map(r => (r(1).toString.split(" ")(1).split(":")(0).toInt, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    TopHoursForRide
      .take(10)
      .foreach(println)

    TopHoursForRide
      .map{case (h, c) => (h.toString() + " " + c.toString)}
      .saveAsTextFile("src/main/resources/data/Res2")
    // --------------------------------------------------------------

    // --------------------------------------------------------------
    // Задание 3
    //   Как происходит распределение поездок по дистанции
    //   (Пример: можно построить витрину со следующими колонками: общее количество поездок
    //    , среднее расстояние
    //    , среднеквадратическое отклонение
    //    , минимальное и максимальное расстояние
    //    )

    val ridesFactDataDataSet = ridesFactData
      .select("trip_distance")
      .as[ridesData]

    val tripDistanceStat = ridesFactDataDataSet
      .select(
        count("trip_distance").alias("total_rides_count"),
        avg("trip_distance").alias("average_ride_distance"),
        stddev ("trip_distance").alias("ride_distance_std"),
        min("trip_distance").alias("min_ride_distance"),
        max("trip_distance").alias("max_ride_distance")
      )

    tripDistanceStat
      .show()

    Class.forName("org.postgresql.Driver")
    val dbUrl = "jdbc:postgresql://localhost:5432/amelchukova"
    val dbProperties = new java.util.Properties()
    dbProperties.setProperty("user", "docker")
    dbProperties.setProperty("password", "docker")

    tripDistanceStat.write
      .mode(SaveMode.Append)
      .jdbc(dbUrl, "a_table", dbProperties)

    // --------------------------------------------------------------

  }

}
