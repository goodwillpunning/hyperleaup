package com.databricks.labs.hyperleaup

import java.nio.file.{Path, Paths}
import collection.JavaConverters._
import com.tableau.hyperapi._


object HyperFileUtils {

  def getTables(schema: String, hyperFilePath: String): Seq[TableName] = {
    var hyperProcess: Option[HyperProcess] = None
    var connection: Option[Connection] = None
    try {
      val hyperDatabase: Path = Paths.get(hyperFilePath)
      hyperProcess = Some(new HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU))
      connection = Some(new Connection(hyperProcess.get.getEndpoint, hyperDatabase.toString))
      val catalog: Catalog = connection.get.getCatalog
      // Query the Catalog API for all tables under the given schema
      catalog.getTableNames(new SchemaName(schema)).asScala
    } catch {
      case e: Throwable => throw e
    } finally {
      connection.map(_.close)
      hyperProcess.map(_.close)
    }
  }

  def getRowCount(schema: String, table: String, hyperFilePath: String): Long = {
    var hyperProcess: Option[HyperProcess] = None
    var connection: Option[Connection] = None
    try {
      val hyperDatabase: Path = Paths.get(hyperFilePath)
      hyperProcess = Some(new HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU))
      connection = Some(new Connection(hyperProcess.get.getEndpoint, hyperDatabase.toString))
      // Query the Hyper File for the number of rows in the table
      connection.get.executeScalarQuery("SELECT COUNT(*) FROM " + new TableName(schema, table)).get()
    } catch {
      case e: Throwable => throw e
    } finally {
      connection.map(_.close)
      hyperProcess.map(_.close)
    }
  }
}
