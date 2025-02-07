# Modelado de Datos en Scala con Spark


1. [Introducción](#schema1)
2. [Creación de una SparkSession](#schema2)
3. [Modelado de Datos con Spark DataFrames y Datasets](#schema3)
4. [Operaciones Básicas](#schema4)
5. [Ejercicio 1: Análisis Simple de Datos de Usuarios](#schema5)
6. [SQL en Spark](#schema6)


<hr>

<a name="schema1"></a>

# Nivel Principiante
# 1. Introducción a Spark y Scala
**Spark** es un motor de procesamiento de datos distribuido que permite manejar grandes volúmenes de información de manera eficiente. Scala es el lenguaje en el que Spark fue originalmente desarrollado, lo que lo hace muy eficiente para este tipo de tareas.

`Scala` es un lenguaje de programación que combina características de programación funcional y orientada a objetos. Se ejecuta en la `JVM (Java Virtual Machine)` y es conocido por su concisión y su capacidad para interactuar con el código Java. Fue creado por Martin Odersky y su principal propósito es mejorar las deficiencias del lenguaje Java mientras mantiene su interoperabilidad.
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

# 3. Modelado de Datos con Spark DataFrames y Datasets
## **Creación de DataFrames**
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



# Guía Completa de Modelado de Datos en Scala con Apache Spark









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