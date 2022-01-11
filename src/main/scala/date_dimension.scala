import org.apache.spark.sql.SparkSession

object date_dimension extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()

  //dataset
  val datePath="src/datasets/raw_layer/date"
  val dateDF = spark.read
    .option("sep", ";")
    .option("header", true)
    .option("inferSchema", true)
    .csv(datePath)

  //Date Staging Dimension
  println("\n Date staging dimension")
  dateDF.printSchema()
  dateDF.show(5,false)

}
