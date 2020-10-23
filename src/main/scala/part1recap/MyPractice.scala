package part1recap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import part1recap.SparkRecap.GuitarPlayer

object MyPractice {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Recap")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    var guitarPlayers = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/guitarPlayers").cache()

    //    guitarPlayers.show(5)

    val upper: String => String = _.toUpperCase
    val upperUDF = udf(upper)
    val addOneUdf = udf { x: Long => x + 1 }

    //    guitarPlayers.withColumn("upper", upperUDF($"name")).show(5)
    //    guitarPlayers.withColumn("upper", addOneUdf($"id")).show(5)

    val guitarPlayersDS = guitarPlayers.as[GuitarPlayer]

    guitarPlayersDS.map(_.name)
    //      .show(5)

    var cars = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/data/cars")

    //    cars.show(5)

    cars.createOrReplaceTempView("cars")
    spark.table("cars")
    //      .show(5)

    val americanCarsSql = spark.sql(
      """
        |select Name from cars where Origin = 'Europe'
    """.stripMargin
    )
    //    americanCarsSql.show(5)

    val americanCarsFilter = cars.filter($"Origin".equalTo("Europe"))
      .map(_.getAs[String]("Name"))

    //    americanCarsFilter.show(5)

    val sc = spark.sparkContext
    val numbersRDD: RDD[Int] = sc.parallelize(1 to 1000000)

    // functional operators
    val doubles = numbersRDD.map(_ * 2)
    //    doubles.foreach(println(_))

    val numbersDF: DataFrame = numbersRDD.toDF("number") // you lose type info, you get SQL capability
    //      numbersDF.show(5)

    val numbersDS: Dataset[Int] = spark.createDataset(numbersRDD)
    //    numbersDS.show(5)

    val list: List[MyTestClass] = List(MyTestClass("Name1", 20), MyTestClass("Name2", 20), MyTestClass("Name3", 20))
    val myClassRDD: RDD[MyTestClass] = sc.parallelize(list)

    // RDD -> DF
    myClassRDD.toDF()
    //      .show()

    // RDD -> DS
    val myClassDS: Dataset[MyTestClass] = spark.createDataset(myClassRDD)
    //    myClassDS.show()

    // DS -> RDD
    val guitarPlayersRDD = guitarPlayersDS.limit(5).rdd
    //    guitarPlayersRDD.foreach(println(_))

    // DF -> RDD
    val carsRDD = cars.limit(5).rdd // RDD[Row]
    carsRDD.foreach(println(_))
  }

}
