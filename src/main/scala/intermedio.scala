import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object intermedio extends App {

  val spark = SparkSession.builder()
    .appName("intermedio")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val ventasDF = spark.read
    .option("header","true")
    .option("inferSchema","true")
    .csv("data/ventas.csv")

  ventasDF.show()

  // Calcular el ingreso total por categoría:
  val ingresosPorCategoria = ventasDF
    .groupBy("categoria")
    .agg(sum($"precio" * $"cantidad").alias("ingresos_total"))

  ingresosPorCategoria.show()

  // Contar la cantidad total de productos vendidos por categoría:

  val productosVendidos = ventasDF
    .groupBy("categoria")
    .agg(sum("cantidad").alias("productos_vendidos"))

  productosVendidos.show()

  // Ranking de ventas por precio dentro de cada categoría:
  val ventanaCategoria = Window.partitionBy("categoria").orderBy($"precio".desc)
  val rankingVentas = ventasDF.withColumn("ranking_precio",rank().over(ventanaCategoria))
  rankingVentas.show()

  // Precio acumulado en cada categoria

  val ventanaAcumulado = Window.partitionBy("categoria").orderBy("precio")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val productoAcumulado = ventasDF
    .withColumn("precio_acumulado",sum("precio").over(ventanaAcumulado))
  productoAcumulado.show()

  // Comparar el salario actual con el salario anterior (lag()) y el siguiente (lead())

  val productoComparacion = ventasDF
    .withColumn("precion_anterior",lag("precio",1).over(ventanaCategoria))
    .withColumn("precios_siguiente", lead("precio",1).over(ventanaCategoria))
  productoComparacion.show()




  spark.stop()





}