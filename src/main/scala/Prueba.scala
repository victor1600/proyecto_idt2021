import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf, upper}

object Prueba extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

}
