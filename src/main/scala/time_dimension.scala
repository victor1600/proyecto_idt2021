import org.apache.spark.sql.SparkSession

object time_dimension extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()

  //dataset
  val timePath="src/datasets/raw_layer/time"
  val timeDF = spark.read
    .option("sep", ";")
    .option("header", true)
    .option("inferSchema", true)
    .csv(timePath)

  //Date Staging Dimension
  println("\n Time staging dimension")
  timeDF.printSchema()
  timeDF.show(5,false)
}
