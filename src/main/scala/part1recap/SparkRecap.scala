package part1recap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

object SparkRecap {

  // the entry point to the Spark structured API
  val spark = SparkSession.builder()
    .appName("Spark Recap")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._


  // read a DF
  var cars = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars")
/*
  //  cars.printSchema()
  cars = cars.limit(15);
//  cars.show()


  // select
  val usefulCarsData = cars.select(
    col("Name"), // column object
    $"Year", // another column object (needs spark implicits)
    (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  //  usefulCarsData.printSchema()
  //  usefulCarsData.show();

  //  val carsWeights = cars.selectExpr("Weight_in_lbs / 2.2")
  val carsWeights = cars.select($"Weight_in_lbs" / 2.2)

  //  carsWeights.show(5);

  // filter
  val europeanCars = cars.where(col("Origin") =!= "USA")

  //  europeanCars.show(10);

  // aggregations
  val averageHP = cars.select(avg(col("Horsepower")).as("average_hp")) // sum, meam, stddev, min, max
  val sumHP = cars.select(sum(col("Horsepower")).as("average_hp")) // sum, meam, stddev, min, max
  val minHP = cars.select(min(col("Horsepower")).as("average_hp")) // sum, meam, stddev, min, max
  val maxHP = cars.select(max(col("Horsepower")).as("average_hp")) // sum, meam, stddev, min, max
  val meanHP = cars.select(mean(col("Horsepower")).as("average_hp")) // sum, meam, stddev, min, max
  val stddevHP = cars.select(stddev(col("Horsepower")).as("average_hp")) // sum, meam, stddev, min, max
  //  println("averageHP")
  //  averageHP.show()
  //  println("sumHP")
  //  sumHP.show()
  //  println("minHP")
  //  minHP.show()
  //  println("maxHP")
  //  maxHP.show()
  //  println("meanHP")
  //  meanHP.show()
  //  println("stddevHP")
  //  stddevHP.show()


  // grouping
  val countByOrigin = cars
    .groupBy(col("Origin")) // a RelationalGroupedDataset
    .count()

  //  countByOrigin.show()*/

  // joining
  var guitarPlayers = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers").cache()

//  guitarPlayers = guitarPlayers.limit(5)
//  guitarPlayers.show()

  var bands = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands")

  bands = bands.limit(5)
//  bands.show()

  val guitaristsBandsInner = guitarPlayers.join(bands, guitarPlayers.col("band") === bands.col("id"))
  val guitaristsBandsLeft = guitarPlayers.join(bands, guitarPlayers.col("band") === bands.col("id"), "left")
  val guitaristsBandsAntiLeft = guitarPlayers.join(bands, guitarPlayers.col("band") === bands.col("id"), "leftanti")

//    guitaristsBandsInner.show()
//    guitaristsBandsLeft.show()
//    guitaristsBandsAntiLeft.show()
  /*
    join types
    - inner: only the matching rows are kept
    - left/right/full outer join
    - semi/anti
   */

  // datasets = typed distributed collection of objects
//  val upper: String => String = _.toUpperCase
//  val upperUDF = udf(upper)
//  val addOneUdf = udf { x: Long => x + 1 }
//
//  guitarPlayers.withColumn("upper", upperUDF($"name")).show(5)
//  guitarPlayers.withColumn("upper", addOneUdf($"id")).show(5)

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  val guitarPlayersDS = guitarPlayers.as[GuitarPlayer] // needs spark.implicits
  guitarPlayersDS.map(_.name)

  // Spark SQL
  cars.createOrReplaceTempView("cars")
  val americanCars = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
    """.stripMargin
  )

  // low-level API: RDDs
  val sc = spark.sparkContext
  val numbersRDD: RDD[Int] = sc.parallelize(1 to 1000000)

  // functional operators
  val doubles = numbersRDD.map(_ * 2)

//  var guitarPlayersDS = guitarPlayers.as[GuitarPlayer].cache() // needs spark.implicits
//  guitarPlayers.explain()

  import org.apache.spark.sql.functions.udf

  //  // Spark SQL
  //  cars.createOrReplaceTempView("cars")
  //  val americanCars = spark.sql(
  //    """
  //      |select Name from cars where Origin = 'USA'
  //    """.stripMargin
  //  )
  //
  //  // low-level API: RDDs
  //  val sc = spark.sparkContext
  //  val numbersRDD: RDD[Int] = sc.parallelize(1 to 1000000)
  //
  //  // functional operators
  //  val doubles = numbersRDD.map(_ * 2)
  //
  //  // RDD -> DF
  //  val numbersDF = numbersRDD.toDF("number") // you lose type info, you get SQL capability
  //
  //  // RDD -> DS
  //  val numbersDS = spark.createDataset(numbersRDD)
  //
  //  // DS -> RDD
  //  val guitarPlayersRDD = guitarPlayersDS.rdd
  //
  //  // DF -> RDD
  //  val carsRDD = cars.rdd // RDD[Row]
  //
  //  def main(args: Array[String]): Unit = {
  //    // showing a DF to the console
  //    cars.show()
  //    cars.printSchema()
  //  }
}
