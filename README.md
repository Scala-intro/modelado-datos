# Guía Completa de Modelado de Datos en Scala con Apache Spark


1. [Introducción](#schema1)
2. [Introducción a Apache Spark](#schema2)
3. [Modelado de Datos con Spark DataFrames y Datasets](#schema3)
4. [Operaciones Básicas](#schema4)
5. [Ejercicio 1: Análisis Simple de Datos de Usuarios](#schema5)
6. [SQL en Spark](#schema6)
7. [Funciones Agregadas y de Ventana (Window Functions)](#schema7)
8. [Diseño de Pipelines ETL](#schema8)
9. [Ejercicio](#schema9)
10. [Introducción a Spark Streaming](#schema10)
11. [Integración con Kafka](#schema11)
12. [Configuración de Clúster de Spark](#schema12)
13. [Optimización Avanzada en Spark](#schema13)
14. [Ejercicio: Optimizar un Pipeline ETL para más de 1M de registros](#schema14)
15. [Integración con Bases de Datos](#schema15)



<hr>

# Nivel 1: Fundamentos de Scala y Apache Spark

<hr>

<a name="schema1"></a>

# 1. Introducción a Spark y Scala
**Spark** es un motor de procesamiento de datos distribuido que permite manejar grandes volúmenes de información de manera eficiente. Scala es el lenguaje en el que Spark fue originalmente desarrollado, lo que lo hace muy eficiente para este tipo de tareas.

`Scala` es un lenguaje de programación que combina características de programación funcional y orientada a objetos. Se ejecuta en la `JVM (Java Virtual Machine)` y es conocido por su concisión y su capacidad para interactuar con el código Java. Fue creado por Martin Odersky y su principal propósito es mejorar las deficiencias del lenguaje Java mientras mantiene su interoperabilidad.
```scala
object HolaMundo {
  def main(args: Array[String]): Unit = {
    println("\u00a1Hola, Mundo!")
  }
}
```

## **Sintaxis básica de Scala**
Scala utiliza una sintaxis que es más concisa que la de Java, pero a la vez mantiene la legibilidad y la estructura ordenada. Algunos de los puntos clave son:
- Definir una variable: En Scala, puedes definir una variable usando `val` (inmutable) o `var` (mutable).
```scala
val x = 10 // valor inmutable
var y = 20 // valor mutable
```
- Tipo de datos: Los tipos pueden ser inferidos por el compilador o explícitos:
```scala
val name: String = "Scala"
val age: Int = 25
```
- Funciones: Las funciones en Scala pueden definirse sin paréntesis para parámetros sin valores.
  - def nombre_funcion(parametro:tipo): tipo_retorno = { instrucciones }
```scala
def greet(): String = "Hello, Scala!"
```

## **Tipos de datos y variables**
Scala es un lenguaje fuertemente tipado, lo que significa que las variables tienen un tipo específico. Algunos de los tipos de datos más comunes en Scala son:

- Numéricos:
  - `Int`: Enteros de 32 bits.
  - `Long`: Enteros de 64 bits.
  - `Double`: Números de punto flotante de 64 bits.
  - `Float`: Números de punto flotante de 32 bits.

- Cadenas:
  - `String`: Representa una secuencia de caracteres
  ```scala
  val greeting: String = "Hola"
  ```

- Booleanos:
  - `Boolean`: `true` o `false`.
  ```scala
    val isActive: Boolean = true
  ```
- Colecciones:
  - `List`: Una lista inmutable.
  ```scala
    val numbers = List(1,2,3,4)
  ```
  - `Array`: Los Array en Scala son colecciones mutables de tamaño fijo. Una vez que se crea el array con un tamaño específico, puedes cambiar los valores de sus elementos pero no el tamaño.
  ```scala
  val arr = Array(1,2,3,4)
  ```
  - `Map`: El Map en Scala es una colección que almacena pares clave-valor. Es mutable de forma predeterminada, pero también puedes usar Map inmutable si no deseas que se modifique.
  ```scala
  val map = Map("a"->, "b"->2)
  ```
  - `ArrayBuffer`: ArrayBuffer es un tipo de colección mutable que actúa como una lista cuyo tamaño puede cambiar dinámicamente, similar a una lista en Python.
  ```scala
  import scala.collection.mutable.ArrayBuffer
  val buffer = ArrayBuffer(1, 2, 3, 4)
  ```
  - `ListBuffer`: Otro tipo de colección mutable en Scala es ListBuffer, que también permite modificar su tamaño (agregar o eliminar elementos) pero es más eficiente en la construcción de listas que ArrayBuffer si se realizan muchas modificaciones.
  ```scala
  import scala.collection.mutable.ListBuffer
  val listBuffer = ListBuffer(1, 2, 3, 4)
  ```
- Tuplas:
  - Scala permite crear `tuplas` que pueden contener diferentes tipos de datos.
  ```scala
  val person = ("John", 25)
  ```

## **Estructuras de control (if, for, while)**
Scala soporta estructuras de control similares a otros lenguajes, como `if`, `for`, y `while`.

- if: la expresión if puede retornar un valor, lo que lo hace diferente de otros lenguajes como Java.
```scala
val x = 10
val result = if (x > 5) "Mayor que 5" else "Menor o igual a 5"
```
- for:El ciclo for en Scala es muy potente y permite recorrer colecciones de manera funcional. Se puede usar para iterar sobre rangos, listas, o incluso aplicar filtros y transformaciones.
```scala
for ( i <- 1 to 5){
  println(i) // Imprime los números del 1 al 5

}
```
También puedes usar un for con filtros:
```scala
for ( i <- 1 to 10 if i % 2 == 0){
  println(i) // Imprime los números pares entre 1 y 10
}
```
- while: El ciclo while es similar a otros lenguajes de programación y ejecuta un bloque de código mientras la condición sea true.
```scala
var i = 0
while (i < 5){
  println(i)
  i += 1 
}
```

### Ejemplo:
```scala
object HolaMundo {
  def main(args: Array[String]): Unit = {
    println("!Hola, Mundo!")
  }
}
```

<hr>

<a name="schema2"></a>

# 2. Introducción a Apache Spark
## **¿Qué es Apache Spark?**
Apache Spark es un motor de procesamiento de datos en clústeres, de código abierto, que permite realizar análisis de grandes volúmenes de datos de manera rápida y eficiente. Spark se diseñó para ser más rápido que Hadoop MapReduce, especialmente en tareas iterativas y en memoria.

### Características clave de Apache Spark:
1. Procesamiento distribuido: Spark permite procesar grandes cantidades de datos de manera distribuida a través de múltiples nodos en un clúster, lo que le da un alto rendimiento.
2. In-Memory Processing: Una de las ventajas más notables de Spark es su capacidad para realizar procesamiento en memoria. Esto significa que puede almacenar los datos en memoria RAM durante el procesamiento, lo que lo hace mucho más rápido que otros motores como Hadoop MapReduce, que dependen de operaciones de disco.
3. Compatibilidad con diferentes fuentes de datos: Spark puede conectarse a una amplia variedad de fuentes de datos, como HDFS, S3, bases de datos relacionales, Kafka, y más.
4. APIs flexibles: Spark soporta APIs en varios lenguajes de programación, como Scala, Python, Java, y R, lo que permite a los desarrolladores trabajar con el lenguaje que prefieran.
5. Modelo de programación flexible: Spark proporciona un modelo de programación que permite realizar tareas como transformaciones de datos, operaciones de agregación, filtrado, etc.

## **Arquitectura básica de Spark**
La arquitectura de Apache Spark se basa en el modelo de programación de "map-reduce", pero mucho más eficiente. Está compuesta por varios componentes que trabajan en conjunto para ejecutar las tareas distribuidas en un clúster. Los principales componentes de la arquitectura de Spark son:
1. Driver: El driver es el componente principal que maneja la ejecución de las aplicaciones Spark. Controla el ciclo de vida de la aplicación y coordina la ejecución de las tareas. Es responsable de enviar las tareas a los nodos del clúster y recoger los resultados.
2. Cluster Manager: El cluster manager es el responsable de gestionar los recursos del clúster y de asignar trabajos a los nodos. Spark puede trabajar con varios gestores de clústeres, como:
  - Standalone (Spark incluido como cluster manager)
  - YARN (de Hadoop)
  - Mesos (un clúster de recursos distribuido de código abierto)
3. Worker Nodes: Los nodos de trabajo son los encargados de ejecutar las tareas enviadas por el driver. Cada worker ejecuta múltiples executors.
4. Executors: Son los procesos que realizan el trabajo real en cada nodo. Un executor ejecuta las tareas y almacena los resultados en memoria o en disco según sea necesario.
5. Tasks: Las tareas son las unidades más pequeñas de trabajo en Spark. Las tareas se dividen en unidades más pequeñas y se distribuyen entre los ejecutores en los nodos del clúster.
   Diagrama básico de la arquitectura de Spark:
```pgsql
   
   +-------------------+
   |     Driver        |
   +-------------------+
   |
   +-------------------+
   | Cluster Manager   |
   +-------------------+
   |
   +-------------------+
   | Worker Nodes      |
   | (Executors)       |
   +-------------------+
   |
   +-------------------+
   |   Tasks (Jobs)    |
   +-------------------+
```

## **SparkContext y SparkSession**
En Spark, `SparkContext` y `SparkSession` son componentes fundamentales para interactuar con el clúster y ejecutar tareas. Sin embargo, han evolucionado a lo largo del tiempo, y `SparkSession` es la forma más moderna de acceder a las funcionalidades de Spark.

### SparkContext:
SparkContext es el punto de entrada principal en una aplicación de Spark y se utiliza para conectarse a un clúster de Spark. Se encargaba de la mayoría de las operaciones en versiones anteriores de Spark antes de la introducción de SparkSession.
- Crear un SparkContext: Específicamente, el SparkContext se utiliza para inicializar el entorno de ejecución y acceder a los recursos del clúster.
   ```scala
  val sc = new SparkContext(conf)
  ```
- Donde `conf` es la configuración de Spark (como la dirección del clúster y otros parámetros).

### SparkSession:
A partir de Spark 2.0, SparkSession se introdujo como la nueva interfaz unificada para trabajar con Spark. Integra el funcionamiento de SparkContext y otros componentes como SQLContext, HiveContext, etc. SparkSession es ahora la forma recomendada de interactuar con Spark, ya que proporciona acceso a todas las funcionalidades de Spark, incluida la API de SQL, DataFrames y Datasets.
- Crear un SparkSession:
  ```scala
  val spark = SparkSession.builder()
        .appName("MiAplicacionSpark")
        .config("spark.some.config.option", "config-value")
        .getOrCreate()
  ```
- Al usar SparkSession, no necesitas instanciar un SparkContext directamente, ya que SparkSession lo maneja internamente.

### Comparación entre SparkContext y SparkSession:
- SparkContext:
  - Usado en versiones anteriores de Spark.
  - Punto de entrada para la ejecución y recursos de Spark.
  - Responsable de la comunicación con el clúster.

- SparkSession:
  - Introducido en Spark 2.0 como una interfaz unificada.
  - Permite trabajar con Spark SQL, DataFrames y Datasets.
  - Internamente maneja SparkContext y otras funcionalidades de Spark.





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


# Nivel 2: Modelado de Datos con Spark DataFrames y Datasets


<hr>

<a name="schema3"></a>

# 3. Modelado de Datos con Spark DataFrames y Datasets
## **Creación de DataFrames**
```scala
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

### Creando el schema
```scala
import org.apache.spark.sql.types._

val schema = StructType(Array(
  StructField("id", IntegerType, nullable = false),
  StructField("nombre", StringType, nullable = true),
  StructField("edad", IntegerType, nullable = true),
  StructField("pais", StringType, nullable = true)
))

val usuariosDF = spark.read
  .option("header", "true")
  .schema(schema)
  .csv("data/usuarios.csv")

usuariosDF.show()
```

## **Transformaciones y Acciones**
En Apache Spark, las operaciones sobre los datos se dividen en transformaciones y acciones. Las transformaciones permiten definir cómo se deben manipular los datos, mientras que las acciones ejecutan esas transformaciones y devuelven un resultado.

### Transformaciones vs Acciones
- Transformaciones: Operaciones que devuelven un nuevo DataFrame o RDD sin ejecutar inmediatamente la operación. Ejemplos: `select`, `filter`, `groupBy`, `map`, `agg`.
- Acciones: Operaciones que desencadenan la ejecución real del plan de cómputo. Ejemplos: `show()`, `collect()`, `count()`, `write()`.

## **Transformaciones**
- `select`: Seleccionar columnas
```scala
val selectDF = df.select("Name","Salary")
selectDF.show()
```
- `filter`: Filtrar filas
```scala
val filteredDF = df.filter($"Salary" > 3500)
filteredDF.show()
```
- `groupBy`: Agrupar datos
```scala
val groupeDF = df.groupBy("Department").count()
groupeDF.show()
```

- `agg`: Agregacoión de datos.
  - Se utiliza para aplicar funciones de agregación como `sum()`, `avg()`, `max()`, `min()`, etc.
```scala
val aggregateDF = df.groupBy("Deparment").agg(avg("Salary").alias("AVG_Salary"))
aggregateDF.show()
```
## **Funciones de Agregación y Transformación**
Spark proporciona muchas funciones integradas para realizar operaciones sobre columnas. Algunas de las más comunes son:
- `sum()`: Suma de valores
- `avg()`: Promedio
- `max()`: Valor máximo
- `min()`: Valor mínimo
- `count()`: Conteo de registros
- `withColumn()`: Agrega o modifica una columna
```scala
val aggDF = df.groupBy("Department")
        .agg(
          max("Salary").alias("Max_Salary"),
          min("Salary").alias("Min_Salary"),
          sum("Salary").alias("Total_Salary")
        )
aggDF.show()
```

### Ejemplo usando `withColumn` para crear una nueva columna:
```scala
// Crear una nueva columna con un bono del 10% del salario
val bonusDF = df.withColumn("Bonus", $"Salary" * 0.10)
bonusDF.show()
````
## **Acciones (para desencadenar la ejecución)**
Después de aplicar las transformaciones, necesitas una acción para obtener resultados. Algunas de las acciones más utilizadas son:
- `show()`: Muestra el contenido del DataFrame en la consola.
- `collect()`: Devuelve todos los datos al driver (cuidado con grandes volúmenes de datos).
- `count()`: Cuenta el número de filas.
- `first()`: Devuelve la primera fila del DataFrame.
```scala  
// Mostrar el DataFrame
  df.show()

// Contar el número de empleados
println(s"Total de empleados: ${df.count()}")

// Obtener la primera fila
println(s"Primer registro: ${df.first()}")
```

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
   - Si la edad es menor de 30, la categoría es "Joven". 
   - Si la edad es 30 o mayor, la categoría es "Adulto".
6. Ordena el DataFrame por categoria_edad y edad de forma ascendente.
7. Muestra el resultado final.

[Código](./src/main/scala/ejercicio1.scala)

<hr>

<a name="schema6"></a>

# 6 SQL en Spark
Apache Spark permite ejecutar consultas SQL sobre DataFrames de manera similar a cómo se haría en bases de datos tradicionales. Para ello, se pueden registrar DataFrames como tablas temporales y luego realizar consultas SQL directamente sobre ellas.

## **1. Registro de DataFrames como Tablas Temporales**

- Antes de ejecutar consultas SQL en Spark, es necesario registrar un DataFrame como una tabla temporal dentro de `SparkSession`. Esto permite que los datos sean accesibles a través de consultas SQL.

- Registramos el DataFrame como una tabla temporal con `createOrReplaceTempView`:
```scala
df.createOrReplaceTempView("employees")

```
## ** 2. Ejecución de Consultas SQL en Spark**

Una vez que la tabla temporal está registrada, podemos usar `spark.sql()` para ejecutar consultas SQL.
1. Seleccionar datos con SELECT
- Podemos usar `SELECT` para obtener datos específicos:
```scala
val resultDF = spark.sql("SELECT Name, Salary FROM employess")
resultDF.show()
```

2. Filtrar datos con `WHERE`

```scala
val highSalaryDF = spark.sql("SELECT Name, Salary FROM employees WHERE Salary > 3500")
highSalaryDF.show()
```


3. Agrupar datos con GROUP BY
   Si queremos contar cuántos empleados hay en cada departamento:
```scala
val departmentCountDF = spark.sql("SELECT Department, COUNT(*) AS Num_Employees FROM employees GROUP BY Department")
departmentCountDF.show()
````

4. Calcular agregaciones con `AVG`, `SUM`, `MAX`, `MIN`
   Podemos calcular el salario promedio por departamento:

```scala
val avgSalaryDF = spark.sql("SELECT Department, AVG(Salary) AS Avg_Salary FROM employees GROUP BY Department")
avgSalaryDF.show()
```


5. Ejemplo de empleados ordenados por salario de forma descendente:

```scala
val sortedDF = spark.sql("SELECT Name, Salary FROM employees ORDER BY Salary DESC")
sortedDF.show()
````
6. `JOIN` entre múltiples tablas

Podemos unir dos DataFrames como si fueran tablas SQL.
```scala
 val joinDF = spark.sql(
    """SELECT e.Name, e.Department, d.Building
       FROM employees e
       JOIN departments d
       on e.Department = d.Department

    """
  )
```

<hr>

<a name="schema7"></a>

# 7. Funciones Agregadas y de Ventana (Window Functions)

## Funciones Agregadas
- Calcular el ingreso total por categoría:
  ```scala
  val ingresosPorCategoria = ventasDF
  .groupBy("categoria")
  .agg(sum($"precio" * $"cantidad").alias("ingresos_total"))
  ```
  - Contar la cantidad total de productos vendidos por categoría:
  ```scala
  val productosVendidos = ventasDF
    .groupBy("categoria")
    .agg(sum("cantidad").alias("productos_vendidos"))
  ```
## Funciones de Ventana (Window Functions)

Las funciones de ventana en Spark permiten realizar cálculos sobre subconjuntos de filas dentro de un DataFrame, sin agrupar los datos como lo haría GROUP BY.

- ¿Para qué sirven?
  - Ranking: Asignar rangos dentro de grupos de datos.
    - Acumulados: Calcular sumas, promedios o conteos progresivos. 
    - Comparaciones: Obtener valores anteriores o siguientes dentro de una ventana.

- ¿Cómo funciona una función de ventana?
Las funciones de ventana trabajan sobre un subconjunto de datos (una "ventana"), definido por una partición (`PARTITION BY`) y un orden (`ORDER BY`).
```scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val ventana = Window
  .partitionBy("columna_particion")   // Define la "ventana" (subgrupo de datos)
  .orderBy("columna_orden")           // Define el orden dentro de la ventana

df.withColumn("nueva_columna", funcionVentana.over(ventana))
```
- Ranking: `row_number()`, `rank()`, `dense_rank()`
  - Las funciones de ranking asignan números de posición dentro de cada partición de la ventana.
  - - Ranking de ventas por precio dentro de cada categoría:
  ```scala
  val ventanaCategoria = Window.partitionBy("categoria").orderBy($"precio".desc)
  val rankingVentas = ventasDF.withColumn("ranking_precio",rank().over(ventanaCategoria))
  ```
- Acumulados: `sum()`, `avg()`, `count()` con `ROWS BETWEEN`
  - Las funciones de ventana permiten acumulados progresivos dentro de una partición.
    - Precio acumulado en cada categoria 
    ```scala
    val ventanaAcumulado = Window.partitionBy("categoria").orderBy("precio")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val productoAcumulado = ventasDF
    .withColumn("precio_acumulado",sum("precio").over(ventanaAcumulado))
    productoAcumulado.show()
    ```

  - rowsBetween(Window.unboundedPreceding, Window.currentRow) acumula desde el primer valor hasta la fila actual.
- Comparaciones con lag() y lead()
   - Ejemplo: Comparar el salario actual con el salario anterior (lag()) y el siguiente (lead())



<hr>

# Nivel 3: Diseño de Pipelines de Datos Masivos

<hr>

<a name="schema8"></a>

# 8. Diseño de Pipelines ETL

Los pipelines `ETL` (Extract, Transform, Load) son fundamentales en el procesamiento de datos masivos. Un buen pipeline permite la ingesta, transformación y almacenamiento de datos de manera eficiente y escalable.

## Extracción de datos desde múltiples fuentes
Los datos pueden provenir de diferentes fuentes, como:
- Archivos: CSV, JSON, Parquet, Avro, ORC.
- Bases de datos relacionales: MySQL, PostgreSQL, SQL Server.
- Bases de datos NoSQL: MongoDB, Cassandra, DynamoDB.
- Sistemas distribuidos: HDFS, Amazon S3, Google Cloud Storage.

En Spark con Scala, la extracción se hace con spark.read:
```scala
val pedidosDF = spark.read.option("header", "true").csv("data/pedidos.csv")
val clientesDF = spark.read.option("header", "true").csv("data/clientes.csv")
```
Si la fuente es una base de datos, usamos jdbc:

```scala
val jdbcDF = spark.read
.format("jdbc")
.option("url", "jdbc:mysql://localhost:3306/miDB")
.option("dbtable", "pedidos")
.option("user", "usuario")
.option("password", "contraseña")
.load()
```
## Transformación compleja de datos
Las transformaciones en Spark se hacen sobre `DataFrames` o `RDDs`. Algunas de las más comunes incluyen:

1. Eliminación de valores nulos

Usamos `filter()` o `na.drop()` para eliminar filas con valores nulos.

```scala
val pedidosLimpios = pedidosDF.na.drop()
```
Si queremos reemplazar valores nulos:

```scala
val pedidosReemplazados = pedidosDF.na.fill(0, Seq("monto"))
```
2. Conversión de tipos de datos
En Spark, todos los datos leídos desde CSV son tipo String. Para convertirlos a otros tipos:

```scala
val pedidosTyped = pedidosDF.withColumn("monto", col("monto").cast("double"))
```
3. Normalización y agregación
Podemos normalizar datos para asegurarnos de que estén en un rango específico o agregar valores por grupo:

```scala
import org.apache.spark.sql.functions._

val pedidosAgrupados = pedidosDF
  .groupBy("cliente_id")
  .agg(sum("monto").alias("total_compras"))
```

## Carga en diferentes destinos
Podemos almacenar los datos en distintos formatos según las necesidades del negocio:
- Parquet (Recomendado para Big Data)
- JSON
- Bases de datos
- S3, HDFS u otros sistemas distribuidos
```scala
resultadoDF.write.mode("overwrite").parquet("output/pedidos_procesados")
```
Si queremos guardar en una base de datos:

```scala
resultadoDF.write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/miDB")
  .option("dbtable", "pedidos_procesados")
  .option("user", "usuario")
  .option("password", "contraseña")
  .mode("append")
  .save()
```

<hr>

<a name="schema9"></a>

# 9. Ejercicio
Crea un pipeline ETL que haga lo siguiente:
- Extraer datos desde dos archivos CSV.
- Transformar los datos (limpiar nulos, convertir tipos, hacer agregaciones).
- Guardar los resultados en formato Parquet.

[ETL Pipeline con Scala y Spark"](./src/main/scala/etl.scala)

<hr>

# Nivel 4: Procesamiento en Tiempo Real con Spark Streaming

<hr>

<a name="schema10"></a>

# 10. Introducción a Spark Streaming

`Spark Streaming` es una extensión de Apache Spark que permite el procesamiento de datos en tiempo real. Permite procesar flujos de datos de manera continua y de forma paralela, similar al procesamiento por lotes (batch), pero con la diferencia de que los datos se procesan en micro-lotes de un tamaño determinado.

## Arquitectura de Spark Streaming
La arquitectura de Spark Streaming se basa en micro-lotes, lo que significa que los datos de un flujo (stream) se dividen en pequeños lotes de tiempo y son procesados por Spark.
1. Entrada de Datos (Stream Sources): Los datos provienen de diversas fuentes como Kafka, Flume, Kinesis, o sockets.
2. Stream Context: El StreamingContext es el componente central que define el procesamiento del flujo.
3. Transformaciones y Acciones: Se pueden aplicar operaciones de transformación a los micro-lotes, como map(), flatMap(), reduce(), etc.
4. Salida (Sinks): Los resultados procesados se pueden almacenar en bases de datos, archivos HDFS, o sistemas de almacenamiento como Amazon S3.

## Creación de flujos de datos en tiempo real
En este ejemplo básico, se crea un flujo de datos que se lee desde un socket y se muestra en la consola:
```scala
import org.apache.spark.streaming._

object SparkStreamingExample {
  def main(args: Array[String]): Unit = {
    
    // Crear un contexto de streaming con intervalo de 5 segundos
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    // Crear un flujo de datos que lee desde un socket en localhost:9999
    val lineas = ssc.socketTextStream("localhost", 9999)

    // Imprimir las líneas que llegan al flujo
    lineas.print()

    // Iniciar el procesamiento
    ssc.start()

    // Esperar hasta que el flujo termine
    ssc.awaitTermination()
  }
}
```
Explicación:

1. `StreamingContext`: El punto de entrada de Spark Streaming. Aquí se establece el intervalo de tiempo de los micro-lotes (5 segundos en este caso).
2. `socketTextStream`: Lee datos desde un socket en un puerto específico (localhost:9999 en este ejemplo).
3. `print()`: Muestra las líneas de texto que llegan al flujo en la consola.
4. `start()`: Inicia el procesamiento del flujo.
5. `awaitTermination(): Mantiene el proceso en ejecución hasta que se termine.


<hr>

<a name="schema11"></a>

# 11 Integración con Kafka

Apache Kafka es una plataforma de mensajería distribuida que permite la transmisión de datos en tiempo real. Spark Streaming tiene integración nativa con Kafka, lo que permite procesar mensajes de Kafka de forma continua.

## Lectura de datos desde Kafka

Para leer datos desde un tópico de Kafka en Spark Streaming, necesitamos agregar las dependencias de Kafka para Spark. La manera básica de leer desde Kafka es:
```scala
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession

object KafkaStreamExample {
  def main(args: Array[String]): Unit = {

    // Crear un SparkSession
    val spark = SparkSession.builder()
      .appName("Kafka Spark Streaming")
      .getOrCreate()

    // Crear un contexto de Streaming
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    // Configuración de Kafka
    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092", // Kafka broker
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-group",
      "auto.offset.reset" -> "latest"
    )

    // Definir el tópico desde el que leeremos los mensajes
    val topics = Array("topic_name")

    // Leer los datos desde Kafka
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // Extraer el valor de los mensajes
    val mensajes = kafkaStream.map(record => record.value)

    // Mostrar los mensajes en la consola
    mensajes.print()

    // Iniciar el procesamiento
    ssc.start()

    // Esperar hasta que se termine el flujo
    ssc.awaitTermination()
  }
}

```

1. Configuración de Kafka:

- `"bootstrap.servers"`: Dirección del servidor Kafka.
- `"key.deserializer"` y `"value.deserializer"`: Especifican cómo deserializar los mensajes (en este caso, como cadenas de texto).
- `"group.id"`: El grupo de consumidores en Kafka.
- `"auto.offset.reset"`: Define qué hacer si no se encuentra un offset previo (por ejemplo, latest lee desde los mensajes más recientes).

2. KafkaUtils.createDirectStream:
- Leemos datos de Kafka directamente en el flujo de Spark Streaming.
- `LocationStrategies.PreferConsistent`: Usa la estrategia de distribución más balanceada.
- `ConsumerStrategies.Subscribe`: Se suscribe al tópico de Kafka especificado. 

3. `map(record => record.value)`: Extrae el valor de cada mensaje recibido.

4. `print()`: Muestra los mensajes en la consola.

## Procesamiento en tiempo real y almacenamiento en bases de datos
Una vez que los datos se leen desde Kafka, podemos realizar cualquier tipo de procesamiento sobre ellos (como análisis, agregaciones, filtrado, etc.). Después, los resultados pueden almacenarse en bases de datos, como MySQL, Cassandra, o HDFS.

Ejemplo de almacenamiento de datos procesados en una base de datos:
```scala
val mensajes = kafkaStream.map(record => record.value)

val resultado = mensajes.transform(rdd => {
  // Realizar transformaciones sobre los datos
  rdd.map(msg => (msg, msg.length)) // Ejemplo de transformar los mensajes
})

resultado.foreachRDD(rdd => {
  rdd.foreachPartition(partition => {
    // Aquí se conecta a la base de datos y guarda los resultados
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/base_datos", "usuario", "contraseña")
    partition.foreach(msg => {
      val stmt = conn.prepareStatement("INSERT INTO tabla_mensajes (mensaje, longitud) VALUES (?, ?)")
      stmt.setString(1, msg._1)
      stmt.setInt(2, msg._2)
      stmt.executeUpdate()
    })
    conn.close()
  })
})

```

1. `transform()`: Permite realizar transformaciones sobre el RDD de cada micro-lote.
2. `foreachRDD()`: Permite acceder a cada RDD de los micro-lotes y procesarlos.
3. Conexión a MySQL: Dentro de foreachPartition, nos conectamos a la base de datos y almacenamos los resultados procesados.

<hr>

# Nivel 5: Entornos Distribuidos y Optimización Avanzada

<hr>

<a name="schema12"></a>

# 12. Configuración de Clúster de Spark

## Configuración de Spark en Modo Standalone
Apache Spark puede ejecutarse en diferentes modos de despliegue:
1. Local (para desarrollo y pruebas).
2. Standalone (clúster dedicado sin dependencia de otros sistemas).
3. YARN (usado con Hadoop).
4. Mesos (gestión avanzada de recursos).
5. Kubernetes (para contenedores).

En el modo Standalone, Spark se ejecuta como un clúster con un Master y varios Workers. Para configurarlo:

1. Instalar y Configurar Spark en Modo Standalone
Si ya tienes Spark instalado en tu máquina, puedes iniciar un clúster Standalone ejecutando:

```bash
# Iniciar el Master
./sbin/start-master.sh

# Iniciar un Worker y conectarlo al Master
./sbin/start-worker.sh spark://<MASTER-IP>:7077  
```
Para ver la interfaz de administración, ve a http://localhost:8080

2. Ejecutar una Aplicación en el Clúster
Podemos ejecutar un job en el clúster con:

```bash
./bin/spark-submit \
--master spark://<MASTER-IP>:7077 \
--deploy-mode client \
--executor-memory 2G \
--total-executor-cores 4 \
mi_script.py
```
Este comando ejecuta mi_script.py en el clúster, asignándole 2GB de RAM por ejecutor y 4 núcleos en total.

## Uso de Docker para Simular Entornos Distribuidos
Para simular un clúster distribuido en una sola máquina, puedes usar Docker con Docker Compose.

1. Crear un Archivo docker-compose.yml para Spark
```yaml
version: '3'
services:
spark-master:
image: bitnami/spark:latest
container_name: spark-master
environment:
- SPARK_MODE=master
ports:
- "8080:8080"
- "7077:7077"

spark-worker:
image: bitnami/spark:latest
container_name: spark-worker
environment:
- SPARK_MODE=worker
- SPARK_MASTER_URL=spark://spark-master:7077
depends_on:
- spark-master
```
2. Iniciar el Clúster
Ejecuta:

```bash
docker-compose up -d
```
Esto iniciará un clúster con un Master y un Worker.

<hr>

<a name="schema13"></a>

# 13. Optimización Avanzada en Spark

## Catalyst Optimizer y Tungsten Engine
Apache Spark optimiza automáticamente la ejecución de consultas mediante Catalyst Optimizer y Tungsten Engine.

- Catalyst Optimizer:
  - Optimiza el plan lógico y físico de ejecución.
  - Mejora consultas SQL y DataFrames. 
  - Reordena filtros y proyecciones para minimizar operaciones costosas. 

- Tungsten Engine:
  - Usa código de bajo nivel para optimizar la ejecución.
  - Maneja memoria de forma eficiente, evitando la recolección de basura de Java (GC).
  - Mejora el rendimiento con ejecución vectorizada.

Ejemplo de Catalyst en acción:

```scala
val df = spark.read.parquet("data.parquet")
df.filter($"edad" > 18).select("nombre", "edad").explain()
```
El comando explain() muestra el plan de ejecución optimizado.


## Tuning de Parámetros de Spark
Para mejorar el rendimiento, ajustamos parámetros en `spark-submit` o en `spark-defaults.conf`:

1. Configuración de memoria

```bash
--executor-memory 4G
--driver-memory 2G
```

2. Ajuste de paralelismo

```bash
--conf spark.default.parallelism=100
--conf spark.sql.shuffle.partitions=200
```
3. Usar cache y persistencia

```scala
val df = spark.read.parquet("data.parquet").cache()
df.count() // Fuerza la carga en memoria
```
4. Repartition vs Coalesce
- `repartition(n)`: Aumenta el número de particiones, útil para procesamiento distribuido.
- `coalesce(n)`: Reduce el número de particiones, útil para escribir en disco.
```scala
val dfRepart = df.repartition(10) // Mejora procesamiento en paralelo
val dfCoalesced = df.coalesce(2)  // Reduce archivos pequeños
```

<hr>

<a name="schema14"></a>

# 14. Ejercicio: Optimizar un Pipeline ETL para más de 1M de registros

1. Cargar un dataset grande (+1M de filas), [NYC Yellow Taxi Trip Data](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)
2. Optimizar la extracción, transformación y carga.
3. Usar técnicas avanzadas de rendimiento.

[Optimized ETL Pipeline](src/main/scala/optimized_etl.scala)

## Optimización 1: Ajustar el Número de Particiones
```scala
// Reparticionar el DataFrame para mejorar el rendimiento en clúster
val repartitionedDF = taxiDF.repartition(8) // Ajusta según el tamaño del dataset
```
- ¿Por qué?
  - Evita particiones demasiado pequeñas que causan overhead.
  - Evita particiones demasiado grandes que generan out-of-memory.
  - Ajusta el número de particiones en función del tamaño de los datos y los recursos disponibles.

## Optimización 2: Cachear los Datos para Reutilización
```scala
import org.apache.spark.sql.functions._

val tarifaPorDiaDF = taxiDF
.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
.groupBy("pickup_date")
.agg(avg("fare_amount").alias("tarifa_promedio"))
.orderBy("pickup_date")
.cache() // ⚡ Almacena en memoria para consultas rápidas

tarifaPorDiaDF.count() // 🔥 Acción para materializar el cache
```
- ¿Por qué?
  - Reduce recomputaciones si reutilizas el DataFrame varias veces.
  - Evita recalcular desde el origen, mejorando la latencia.
    
## Optimización 3: Escribir en Parquet con Compresión
```scala
tarifaPorDiaDF.write
.mode("overwrite")
.option("compression", "snappy") // 🔥 Usa compresión Snappy para lecturas rápidas
.parquet("data/tarifa_por_dia_optimized.parquet")
```
- ¿Por qué?
  - Parquet ya es columnar, pero Snappy mejora aún más las lecturas.
  - Reduce el tamaño del archivo sin afectar el rendimiento.
## Optimización 4: Usar coalesce() Antes de Guardar
Si el número de archivos Parquet generados es demasiado alto, usa coalesce() para reducirlos:

```scala
tarifaPorDiaDF
.coalesce(4) // 🔥 Reduce la cantidad de archivos generados
.write.mode("overwrite").parquet("data/tarifa_por_dia_optimized.parquet")
```
- ¿Por qué?
  - Menos archivos significa menos overhead en la lectura.
  - Útil cuando escribes en HDFS, S3 o Data Lake.
## Optimización 5: Evitar la Inferencia Automática de Schema
En lugar de inferSchema, define el schema manualmente para evitar el overhead de lectura:

```scala
import org.apache.spark.sql.types._

val schema = StructType(Array(
StructField("VendorID", IntegerType, true),
StructField("tpep_pickup_datetime", TimestampType, true),
StructField("tpep_dropoff_datetime", TimestampType, true),
StructField("passenger_count", IntegerType, true),
StructField("trip_distance", DoubleType, true),
StructField("fare_amount", DoubleType, true)
))

val taxiDF = spark.read
.option("header", "true")
.schema(schema) // 🔥 Especificar el schema evita la inferencia costosa
.csv("data/yellow_tripdata_2015-01.csv")
```
- ¿Por qué?
  - Evita que Spark escanee todo el archivo para inferir los tipos.
  - Reduce el tiempo de lectura inicial, útil en grandes datasets.



<hr>



# Nivel 6:  Integración con Bases de Datos

<hr>

<a name="schema15"></a>

# 15. Integración con Bases de Datos

Uno de los aspectos clave en la ingeniería de datos es la integración con bases de datos, tanto relacionales como NoSQL. Spark facilita esta integración mediante conectores específicos para cada tipo de almacenamiento.

## Conexión a bases de datos relacionales (JDBC)
Spark permite conectarse a bases de datos relacionales como `PostgreSQL`, `MySQL`, `SQL Server` y `Oracle` mediante JDBC. Esto permite leer y escribir datos en estas bases de datos de manera eficiente.

### Ejemplo: Conectar Spark a PostgreSQL
```scala
val jdbcUrl = "jdbc:postgresql://localhost:5432/mi_base_de_datos"
val connectionProperties = new java.util.Properties()
connectionProperties.setProperty("user", "usuario")
connectionProperties.setProperty("password", "contraseña")
connectionProperties.setProperty("driver", "org.postgresql.Driver")

// Leer desde PostgreSQL
val df = spark.read
.jdbc(jdbcUrl, "tabla_ejemplo", connectionProperties)

df.show()

// Escribir en PostgreSQL
df.write
.mode("append") // También puede ser "overwrite"
.jdbc(jdbcUrl, "tabla_destino", connectionProperties)
```
### Consideraciones:

- JDBC no es eficiente para grandes volúmenes de datos, por lo que se recomienda usar particionamiento (`partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`).
- Es posible optimizar las escrituras usando `batch inserts`.

## Lectura y escritura en NoSQL (Cassandra, MongoDB)
Muchas arquitecturas de big data utilizan bases de datos NoSQL para el almacenamiento de datos estructurados y semiestructurados. Spark se integra fácilmente con bases de datos como `Cassandra` y `MongoDB`.

### Ejemplo: Integración con Cassandra
Para leer y escribir datos en Apache Cassandra, se utiliza el conector `spark-cassandra-connector`.
```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("SparkCassandraExample")
  .config("spark.cassandra.connection.host", "127.0.0.1")
  .getOrCreate()

// Leer datos de Cassandra
val df = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "usuarios", "keyspace" -> "mi_keyspace"))
  .load()

df.show()

// Escribir datos en Cassandra
df.write
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "usuarios_destino", "keyspace" -> "mi_keyspace"))
  .mode("append")
  .save()
```
### Consideraciones:

- Cassandra es altamente escalable y se recomienda para casos donde la latencia de lectura no es crítica.
- Se debe elegir una clave de partición adecuada para evitar hotspots en los nodos.

### Ejemplo: Integración con MongoDB
Para leer y escribir en MongoDB, se usa el conector `spark-mongodb-connector`.

```scala
val mongoUri = "mongodb://127.0.0.1/mi_base.mi_coleccion"

val dfMongo = spark.read
.format("mongo")
.option("uri", mongoUri)
.load()

dfMongo.show()

// Escribir en MongoDB
dfMongo.write
.format("mongo")
.option("uri", mongoUri)
.mode("append")
.save()
```
### Consideraciones:

- MongoDB es útil para almacenar datos semiestructurados, como JSON o documentos anidados.
- Spark permite filtrar, transformar y agregar datos directamente sobre MongoDB.



E