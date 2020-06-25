package com.databricks.labs.hyperleaup

class HyperFileTestSuite extends org.scalatest.FunSuite with SparkSessionFixture {

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  test("A Hyper File should be created from an input DataFrame") {

    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
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

}