import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object monthly_flight_fact extends App{

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()


  //Airline dimension
  val airlinePath="src/datasets/raw_layer/airline"
  val airlineDF = spark.read
    .option("sep", ";")
    .option("header", true)
    .option("inferSchema", true)
    .csv(airlinePath)

  //Airport dimension
  val airportPath="src/datasets/raw_layer/airport"
  val airportDF = spark.read
    .option("sep", ";")
    .option("header", true)
    .option("inferSchema", true)
    .csv(airportPath)


}
