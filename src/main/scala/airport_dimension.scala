import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object airport_dimension extends App{

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()




}
