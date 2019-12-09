package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  // counting
  val genresCount = moviesDF
    .select(
      count(col("Major_Genre"))
    )

  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))

  // sum and avg
  moviesDF.select(sum(col("IMDB_Rating")))

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  // Grouping

  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
      .agg(
        count("*").as("N_Movies"),
        avg("IMDB_Rating").as("Avg_Rating")
      )
}
