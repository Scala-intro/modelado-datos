import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object EjemploSql extends App {
  val spark = SparkSession.builder()
    .appName("EjemploSql")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  // Crear un DataFrame de ejemplo
  val data = Seq(
    (1, "Alice", "Marketing", 3000),
    (2, "Bob", "IT", 4000),
    (3, "Charlie", "Marketing", 3500),
    (4, "David", "IT", 4500),
    (5, "Eva", "HR", 3800)
  )

  val df = data.toDF("ID","Name","Department","Salary")

  df.show()

  df.createOrReplaceTempView("employees")

  // Segundo DataFrame

  val departmentData = Seq(
    ("Marketing", "Building A"),
    ("IT", "Building B"),
    ("HR", "Building C")
  )
  val deptDF = departmentData.toDF("Department","Building")

  deptDF.createOrReplaceTempView("departments")

  // Ejemplos

  val resultDF = spark.sql("SELECT Name, Salary FROM employees")
  resultDF.show()

  val highSalary = spark.sql("SELECT Name, Salary FROM employees WHERE Salary > 3500")
  highSalary.show()

  val departmentCountDF = spark.sql("SELECT Department, COUNT(*) as Num_Employees FROM employees GROUP BY Department")
  departmentCountDF.show()

  val avgSalaryDF = spark.sql("SELECT Department, AVG(Salary) as AVG_Salary FROM employees GROUP BY Department")
  avgSalaryDF.show()

  val sortedDF = spark.sql("SELECT Name, Salary FROM employees ORDER BY Salary DESC")
  sortedDF.show()


  val joinDF = spark.sql(
    """SELECT e.Name, e.Department, d.Building
       FROM employees e
       JOIN departments d
       on e.Department = d.Department

    """
  )

  joinDF.show()

}