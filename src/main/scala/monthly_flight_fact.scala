import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object monthly_flight_fact extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()


  //Airline dimension
  val airlinePath = "src/datasets/presentation_layer/airline_dimension"
  val airlineDF = spark.read
    .parquet(airlinePath)

  //Airport dimension
  val airportPath = "src/datasets/presentation_layer/airport_dimension"
  val airportDF = spark.read
    .parquet(airportPath)

  //Montlhy dimension
  val montlhyPath = "src/datasets/presentation_layer/monthly_dimension"
  val monthlyDF = spark.read
    .parquet(montlhyPath)

  //Dataset passengers
  val passengersPath = "src/datasets/raw_layer/passengers"
  val passengersDF = spark.read
    .option("sep", ",")
    .option("header", true)
    .option("inferSchema", true)
    .csv(passengersPath)

  import spark.implicits._
  //Origin airport

  val airportOriginDF = airportDF.select(
    $"airport_key" as "origin_airport_key",
    $"code" as "origin_airport_id",
    $"airport_name")



  val airportDestDF = airportDF.select(
    $"airport_key" as "dest_airport_key",
    $"code" as "dest_airport_id")
    .where(col("current_flag") === true )



//Factable Monthly Flights
  val monthlyFactDF =  passengersDF
    .join(airlineDF, passengersDF("carrier") === airlineDF("carrier"), "left")
    .join(airportOriginDF, passengersDF("origin_airport_id") === airportOriginDF("origin_airport_id"), "left")
    .join(airportDestDF, passengersDF("dest_airport_id") === airportDestDF("dest_airport_id"), "left")
    .join(monthlyDF, passengersDF("month") === monthlyDF("month")  &&
          passengersDF("year") === monthlyDF("year"),"left")
    .select(
   $"airline_key",
         $"freight",
         $"passengers",
         $"mail",
         $"origin_airport_key",
         $"dest_airport_key",
         $"monthly_key")

   .distinct()

  //test
    monthlyFactDF.show(false)
  val qFact = monthlyFactDF.count()
  print("\n Rows df: ", qFact)

  val fact_monthly_flight_loc = "src/datasets/presentation_layer/monthly_flight_fact_table"

  //write rows
  monthlyFactDF
    .write
    .option("compression", "snappy")
    .mode("overwrite")
    .parquet(fact_monthly_flight_loc)

  val parquetMonthlyFact = spark.read.parquet(fact_monthly_flight_loc)
  val qMonthlyFact = parquetMonthlyFact.count()
  print("\n Rows - presentation_layer : ", qMonthlyFact)
}



