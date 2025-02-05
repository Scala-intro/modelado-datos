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

`Scala` es un lenguaje de programación que combina características de programación funcional y orientada a objetos. Se ejecuta en la `JVM (Java Virtual Machine)` y es conocido por su concisión y su capacidad para interactuar con el código Java. Fue creado por Martin Odersky y su principal propósito es mejorar las deficiencias del lenguaje Java mientras mantiene su interoperabilidad.

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
