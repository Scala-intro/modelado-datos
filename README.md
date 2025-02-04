# Modelado de Datos en Scala con Spark


1. [Introducción](#schema1)
2. [Creación de una SparkSession](#schema2)
3. [Cargar Datos en un DataFrame](#schema3)
4. [Operaciones Básicas](#schema4)
5. [Ejercicio 1: Análisis Simple de Datos de Usuarios](#schema5)



<hr>

<a name="schema1"></a>

# Nivel Principiante
# 1. Introducción a Spark y Scala
**Spark** es un motor de procesamiento de datos distribuido que permite manejar grandes volúmenes de información de manera eficiente. Scala es el lenguaje en el que Spark fue originalmente desarrollado, lo que lo hace muy eficiente para este tipo de tareas.

<hr>

<a name="schema2"></a>

# 2. Creación de una SparkSession
La `SparkSession` es el punto de entrada para trabajar con DataFrames en Spark.
```scala
import org.apache.spark.sql.SparkSession

object ModeladoDeDatosPrincipiante extends App {
  val spark = SparkSession.builder()
    .appName("ModeladoDeDatosPrincipiante")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  println("¡Sesión de Spark iniciada!")

  spark.stop()
}
```

- `SparkSession.builder()`: Inicia el proceso de configuración de la sesión Spark.
- `.appName("ModeladoDeDatosPrincipiante")`: Define un nombre para la aplicación. Esto lo verás reflejado en la interfaz de Spark (en localhost:4040 si está habilitada).
- `.master("local[*]")`: Indica que la aplicación se ejecutará en modo local utilizando todos los núcleos de CPU disponibles (*).
- `.getOrCreate()`: Crea una nueva sesión de Spark si no existe; si ya hay una activa, la reutiliza.

<hr>

<a name="schema3"></a>

# 3. Cargar Datos en un DataFrame

```
val usuariosDF = spark.read
  .option("header", "true")   // Indica que la primera fila tiene nombres de columnas
  .option("inferSchema", "true") // Inferir automáticamente el tipo de datos
  .csv("ruta/al/archivo/usuarios.csv")

// Mostrar el contenido del DataFrame
usuariosDF.show()
```
- `val usuariosDF`
    - Declara una variable inmutable llamada usuariosDF, que almacenará un DataFrame.
    - Un DataFrame es una estructura de datos tabular similar a una hoja de Excel o una tabla de base de datos.
- `spark.read`
  - Usa la SparkSession para leer datos desde una fuente externa. En este caso, un archivo CSV.
- `.option("header", "true")`
  - Le indica a Spark que la primera fila del CSV contiene los nombres de las columnas.


<hr>

<a name="schema4"></a>

# 4. Operaciones Básicas
- Seleccionar columnas específicas:
```scala
usuariosDF.select("nombre", "ciudad").show()
```
- Filtrar datos (usuarios mayores de 25 años):
```scala
usuariosDF.filter($"edad" > 25).show()
```
- Agregar una nueva columna (edad + 5 años):
```scala
import org.apache.spark.sql.functions._

val usuariosConEdadFutura = usuariosDF.withColumn("edad_futura", $"edad" + 5)
usuariosConEdadFutura.show()
```
- Ordenar los datos por edad (descendente):
```scala
usuariosDF.orderBy($"edad".desc).show()
```
- `$"edad"`
    - `"edad"`: Es el nombre de la columna en tu DataFrame (usuariosDF).
    - `$`: Es un interpolador de strings proporcionado por Spark para crear una instancia de la clase Column de forma rápida. 
    - Equivalente largo: `$"edad"` es lo mismo que `col("edad")`, donde `col` es una función de `org.apache.spark.sql.functions.`
      Para que $"edad" funcione, necesitas importar:

- ¿Cómo funciona internamente?

```scala
import spark.implicits._
```
Este `import` habilita la interpolación de columnas en `Spark` usando `$`, simplificando la sintaxis al trabajar con `DataFrames.`

<hr>

<a name="schema5"></a>

# 5. Ejercicio 1: Análisis Simple de Datos de Usuarios
1. Carga el archivo usuarios.csv en un DataFrame.
2. Filtra a los usuarios que vivan en "Madrid" o "Bogotá".
3. Crea una nueva columna llamada categoria_edad donde:
4. Si la edad es menor de 30, la categoría es "Joven".
5. Si la edad es 30 o mayor, la categoría es "Adulto".
6. Ordena el DataFrame por categoria_edad y edad de forma ascendente.
7. Muestra el resultado final.

[Código](./src/main/scala/ejercicio1.scala)
