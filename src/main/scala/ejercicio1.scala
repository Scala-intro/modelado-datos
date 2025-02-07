import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ejercicio1 extends App {
  val spark = SparkSession.builder()
    .appName("Ejercicio1")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  // Carga el archivo usuarios.csv en un DataFrame.
  val usuariosDF = spark.read
    .option("header","true")
    .option("inferSchema","true")
    .csv("data/usuarios.csv")

  usuariosDF.show()
  // Filtra a los usuarios que vivan en "Madrid" o "Bogotá".
  val usuariosFiltrados = usuariosDF.filter($"ciudad".isin("Madrid","Bogotá"))
  //usuariosFiltrados.show()
  val usuariosFiltrados2 = usuariosDF.filter($"ciudad" === "Madrid" || $"ciudad" === "Bogotá")
  //usuariosFiltrados2.show()
   // Crear una nueva columna `categoria_edad`
  val usuariosConEdad = usuariosFiltrados.withColumn("categoria_edad",
    when($"edad"< 30, "Joven").otherwise("Adulto")
  )
  usuariosConEdad.show()
  val usuariosConEdad2 = usuariosDF.withColumn(
    "categoria_eda",
    expr("CASE WHEN edad < 30 THEN 'Joven' ELSE 'Adulto' END")
  )

  // Ordena el DataFrame por categoria_edad y edad de forma ascendente.
  val usuariosOrdenCategoriaEdad = usuariosConEdad.orderBy($"categoria_edad", $"edad")
  usuariosOrdenCategoriaEdad.show()

  spark.stop()
}