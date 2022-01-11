import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

object monthly_dimension_staging extends App {
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



  //Monthly Staging Dimension
  val monthlyStaging = dateDF.withColumn("monthly_key", monotonically_increasing_id + 1)
    .select(col("monthly_key"), col("calendar_month"), col("calenda_quarter"), col("calendar_year"))

  val writeRows = monthlyStaging.select("calendar_month").count()
  println("\n Meses del año \n", writeRows)



  println("\n Monthly staging dimension")
  monthlyStaging.printSchema()
  monthlyStaging.show(5,false)

  val qStaging = monthlyStaging.count()
  print("Monthly data- staging", qStaging)

  //Write Staging
  val monthlyStaging_location = "src/datasets/staging_layer/monthly"
  //sobreescribiendo en parquet
  monthlyStaging
    .write
    .option("compression", "snappy")
    .format("parquet")
    .mode("overwrite")
    .parquet(monthlyStaging_location)

  val testParquetStaging = spark.read.parquet(monthlyStaging_location)
  testParquetStaging.show(false)
  testParquetStaging.printSchema()

  val qParquetStaging = testParquetStaging.select("monthly_key").distinct().count()
  println("\n Staging data quantities ", qParquetStaging)
}
