package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import part2dataframes.DataFramesBasics.spark

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
    StructField("Year", DateType),
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
    //.schema(carsSchema) // enforce a schema
    .option("inferSchema", "true") // infer a schema
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

  // JSON flags
  spark.read
    //format("json") -> if this is used, you need to do a .load() in the end
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if spark fails parsing it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    //.format("csv")  -> if this is used, you need to do a .load() in the end
    .schema(stockSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet") // parquet is default

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /*
    Exercise: read the movies DF, then write it as
      - tab-separated values file
      - snappy Parquet
      - table public.movies in the Postgres DB
   */

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    .option("nullValue", "")
    .option("path", "src/main/resources/data/movies_dupe.csv")
    .save()

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet") // parquet is default

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()

}
