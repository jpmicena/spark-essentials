package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
      - format
      - schema (optional)
      - path
      - zero or more options
   */

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    //.option("inferSchema", "true") // infer a schema
    .option("mode", "failFast") // dropMalformed (ignore faulty rows), permissive (default)
    .load("src/main/resources/data/cars.json")

  // Alternative
  val carsDFWithOptionMap = spark.read
      .format("json")
      .options(Map(
        "mode" -> "failFast",
        "path" -> "src/main/resources/data/cars.json",
        "inferSchema" -> "true"
      ))
      .load()

  /*
   Writing DFs
    - format
    - save mode = overwrite, append, ignore, errorIfExists
    - path
    - zero or more options
   */

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_dupe.json")
    .save()
}
