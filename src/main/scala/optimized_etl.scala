import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object OptimizedEtl {
  def main (args: Array[String]) : Unit ={
    val spark = SparkSession.builder()
     .appName("Optimized ETL Pipeline")
      .master("local[*]")
     .config("spark.sql.shuffle.partitions","200")// Ajuste de particiones
     .config("spark.executor.memory","4g") // Ajuste de memoria
     .getOrCreate()
"""
    val schema = StructType(Array(
      StructField("VendorID", IntegerType, true),
      StructField("tpep_pickup_datetime", TimestampType, true),
      StructField("tpep_dropoff_datetime", TimestampType, true),
      StructField("passenger_count", IntegerType, true),
      StructField("trip_distance", DoubleType, true),
      StructField("fare_amount", DoubleType, true)
    ))
"""
    // leer el archivo csv
    val taxiDF = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("dataNYC/yellow_tripdata_2015-01.csv",
        "dataNYC/yellow_tripdata_2016-01.csv",
        "dataNYC/yellow_tripdata_2016-02.csv",
        "dataNYC/yellow_tripdata_2016-03.csv")

    // Reparticionar el DataFrame para mejorar el rendimiento en clúster
    val repartitionDF = taxiDF.repartition((8))

    // Transformaciones con optimicación
    repartitionDF.select(taxiDF.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)):_*).show()
    val taxiDFClean = repartitionDF.na.drop()

    val taxiDFDate = taxiDFClean.withColumn("pickup_date",to_date(col("tpep_pickup_datetime")))

    val tarifaPorDiaDF =  taxiDFDate
      .groupBy("pickup_date")
      .agg(
        avg("fare_amount").alias("tarifa_promedio")
      )
      .orderBy("pickup_date")
      .cache()
    tarifaPorDiaDF.count()

    // Optimización 3 y 4: Usar coalesce() y escribir en Parquet con compresión Snappy

    tarifaPorDiaDF.coalesce(4)
      .write
      .mode("overwrite")
      .option("compression","snappy")
      .parquet("dataNYC/tarifa_por_dia_optimized.parquet")


    tarifaPorDiaDF.show(10)

    spark.stop()
  }
}