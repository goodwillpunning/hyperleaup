package com.databricks.labs.hyperleaup

import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType, StringType, StructField, StructType}

class HyperFileTestSuite extends org.scalatest.FunSuite with SparkSessionFixture {

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  test("A Hyper File should be created from an input DataFrame") {

    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9),
      (10, 11, 12),
      (13, 14, 15)
    ).toDF("id", "firstname", "lastname")

    val hyperFile = HyperFile("employees", testDF)

    assert(hyperFile.name == "employees")
  }

  test("A Hyper File should be created from a SQL statement") {

    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("id", "firstname", "lastname")
    testDF.createOrReplaceTempView("employees")

    val sql = "select * from employees"

    val hyperFile = HyperFile("employees", sql)

    assert(hyperFile.name == "employees")
    assert(hyperFile.sql == sql)
  }

  test("A Hyper File should be create from a DataFrame containing Decimal data types") {

    val testDF = Seq(
      (1L, 1, "John", "Doe", 1250, 150.00),
      (1L, 3, "Jane", "Smith", 2500, 225.00),
      (1L, 3, "Joffri", "Banes", 6500, 750.00)
    ).toDF("id", "dept_id", "firstname", "lastname", "salary", "bonus")
     .createOrReplaceTempView("employee_salaries")

    val testDecimals = spark.sql("select id, dept_id, firstname, lastname, cast(salary as decimal(10,0)), bonus from employee_salaries")

    val hyperFile = HyperFile("employee_salaries", testDecimals)

    assert(hyperFile.name == "employee_salaries")
  }

  test("Ensure that a Hyper File's path is set correctly") {
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("id", "firstname", "lastname")

    val hyperFile = HyperFile("employees", testDF)

    // the Hyper File should end with the name + '.hyper' extension
    assert(hyperFile.path.endsWith(s"${hyperFile.name}.hyper"))
  }

  test("Ensure that the Hyper File contains expected data") {
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("id", "firstname", "lastname")
    val hyperFile = HyperFile("employees", testDF)
    val tables = HyperFileUtils.getTables(hyperFile.schema, hyperFile.path)
    val rowCount = HyperFileUtils.getRowCount(hyperFile.schema, hyperFile.table, hyperFile.path)
    assert(tables.size == 1)
    assert(rowCount == 3)
  }

}