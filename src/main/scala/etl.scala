import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ETL_Pipeline {

  def main(args: Array[String]) : Unit = {


    val spark = SparkSession.builder()
      .appName("ETL Pipeline con Scala y Spark")
      .master("local[*]")
      .getOrCreate()

    // Extraer datos desde dos archivos CSV.
    val pedidosDF = spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("data/pedidos.csv")

    val clientesDF = spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("data/clientes.csv")

    pedidosDF.show()
    clientesDF.show()
    // Transformar los datos (limpiar nulos, convertir tipos, hacer agregaciones).
    val pedidosLimpios = pedidosDF.na.drop()
    val pedidosTyped = pedidosLimpios.withColumn("monto",col("monto").cast("double"))
    val pedidosAgrupados = pedidosTyped.groupBy("cliente_id").agg(sum("monto").alias("total_compras"))
    val resultadoDF = pedidosAgrupados.join(clientesDF,"cliente_id")
    resultadoDF.show()

    // Guardar los resultados en formato Parquet.
    resultadoDF.write.mode("overwrite").parquet("output/pedidos_procesados")


    spark.stop()

  }
}