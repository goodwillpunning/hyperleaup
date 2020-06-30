package com.databricks.labs.hyperleaup

import com.databricks.labs.hyperleaup.utils.SparkSessionWrapper
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
  * A Tableau Hyper File
  */
class HyperFile extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  private var _name: String = _
  private var _sql: String = _
  private var _df: DataFrame = _
  private var _schema: String = _
  private var _table: String = _
  private var _path: String = _

  private def setFilename(name: String): this.type = {
    _name = name
    this
  }

  private def setSql(sql: String): this.type = {
    _sql = sql
    this
  }

  private def setDF(value: DataFrame): this.type = {
    _df = value
    this
  }

  private def setDF(sql: String): this.type = {
    _df = spark.sql(sql)
    this
  }

  private def setSchema(schema: String): this.type = {
    _schema = schema
    this
  }

  private def setTable(table: String): this.type = {
    _table = table
    this
  }

  private def setPath(path: String): this.type = {
    _path = path
    this
  }

  /**
    * Create a Tableau Hyper File from Spark DataFrame
    */
  private def createHyperFile(): String = {
    Creator(this).create()
  }

  def name: String = _name

  def sql: String = _sql

  private[hyperleaup] def getDf: DataFrame = _df

  def schema: String = _schema

  def table: String = _table

  def path: String = _path
}

object HyperFile {

  def apply(filename: String, sql: String): HyperFile = {
    val hyperFile = new HyperFile()
      .setFilename(filename)
      .setSql(sql)
      .setDF(sql)
      .setSchema("Extract")
      .setTable("Extract")
    val path = hyperFile.createHyperFile()
    hyperFile.setPath(path)
  }


  def apply(filename: String, df: DataFrame): HyperFile = {
    val hyperFile = new HyperFile()
      .setFilename(filename)
      .setSql("")
      .setDF(df)
      .setSchema("Extract")
      .setTable("Extract")
    val path = hyperFile.createHyperFile()
    hyperFile.setPath(path)
  }

}
