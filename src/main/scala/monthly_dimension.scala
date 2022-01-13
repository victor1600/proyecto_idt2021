import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object monthly_dimension extends App {
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
  dateDF.show(false)

  //Derivated Dimension
  val monthlyDataDF = dateDF
    .select(
      col("calendar_month").alias("month"),
      col("calenda_quarter").alias("quarter"),
      col("calendar_year").alias("year")).distinct()
    .orderBy("calendar_year")

  //test
  monthlyDataDF.show(false)
  val qStaging = monthlyDataDF.count()
  print("Monthly data- staging", qStaging)



  //Monthly dimension
  val monthlyStagingDF = monthlyDataDF
    .withColumn("monthly_key", monotonically_increasing_id + 1)
    .orderBy("calendar_year")


  //test
  monthlyStagingDF.show(false)
  val qS = monthlyStagingDF.count()
  print("Monthly data- staging", qS)


  //Write Staging
  val monthlyStaging_location = "src/datasets/staging_layer/monthly_staging"
  //sobreescribiendo en parquet
  monthlyStagingDF
    .write
    .option("compression", "snappy")
    .format("parquet")
    .mode("overwrite")
    .parquet(monthlyStaging_location)

  //Write Presentation
  val monthly_location = "src/datasets/presentation_layer/monthly_dimension"
  //sobreescribiendo en parquet
  monthlyStagingDF
    .write
    .option("compression", "snappy")
    .format("parquet")
    .mode("overwrite")
    .parquet(monthly_location)



  val testParquetStagingDF = spark.read.parquet(monthlyStaging_location)
  testParquetStagingDF.show(false)
  testParquetStagingDF.printSchema()

  val qParquetStaging = testParquetStagingDF.select("monthly_key").distinct().count()
  println("\n Staging data quantities ", qParquetStaging)


}
