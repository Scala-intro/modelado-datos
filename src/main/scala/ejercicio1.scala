import org.apache.spark.sql.SparkSession

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


  spark.stop()
}