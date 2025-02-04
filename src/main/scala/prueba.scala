import org.apache.spark.sql.SparkSession

object ModeladoDeDatosPrincipiante extends App {
  val spark = SparkSession.builder()
    .appName("ModeladoDeDatosPrincipiante")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Información sobre Spark
  println(spark)  // Muestra el identificador de la instancia
  println(s"App Name: ${spark.sparkContext.appName}")
  println(s"Master: ${spark.sparkContext.master}")
  println(s"Versión de Spark: ${spark.version}")
  println(s"Configuraciones: ${spark.conf.getAll}")
  println(s"SparkContext activo: ${!spark.sparkContext.isStopped}")

  val usuariosDF = spark.read
    .option("header", "true")   // Indica que la primera fila tiene nombres de columnas
    .option("inferSchema", "true") // Inferir automáticamente el tipo de datos
    .csv("data/usuarios.csv")

  // Mostrar el contenido del DataFrame
  usuariosDF.show()

  // Seleccionar columnas específicas:
  usuariosDF.select("nombre", "ciudad").show()
  //usuariosDF.select("edad","nombre","ciudad").show()

  // Filtrar datos (usuarios mayores de 25 años):
  usuariosDF.filter($"edad" > 25).show()

  //Agregar una nueva columna (edad + 5 años):

  import org.apache.spark.sql.functions._

  val usuariosConEdadFutura = usuariosDF.withColumn("edad-futura",$"edad" + 5)
  usuariosConEdadFutura.show()

  // Ordenar los datos por edad (descendente):
   usuariosDF.orderBy($"edad".desc).show()

  // Código equivalente:
   import org.apache.spark.sql.functions._

  usuariosDF.orderBy(col("edad").desc).show()








  spark.stop()
}
