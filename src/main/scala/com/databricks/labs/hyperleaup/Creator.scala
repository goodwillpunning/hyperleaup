package com.databricks.labs.hyperleaup

import com.databricks.labs.hyperleaup.utils.SparkSessionWrapper
import com.tableau.hyperapi.{Catalog, Connection, CreateMode, HyperProcess, Inserter, SchemaName, SqlType, TableDefinition, TableName, Telemetry}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import com.tableau.hyperapi.Nullability.{NOT_NULLABLE, NULLABLE}
import java.nio.file.{Files, Path, Paths}


class Creator(hyperFile: HyperFile) extends SparkSessionWrapper {

  private val _name: String = hyperFile.name
  private val _schema: String = hyperFile.schema
  private val _table: String = hyperFile.table
  private val _path: String = hyperFile.path
  private val _df: DataFrame = hyperFile.getDf

  /**
    *
    */
  private def convertStructField(column: StructField): TableDefinition.Column = {
    val name = column.name
    val dataType = column.dataType match {
      case StringType => SqlType.text()
      case IntegerType => SqlType.integer()
      case LongType => SqlType.bigInt()
      case DoubleType => SqlType.doublePrecision()
      case FloatType => SqlType.doublePrecision()
      case BooleanType => SqlType.bool()
      case DateType => SqlType.date()
      case TimestampType => SqlType.timestamp()
      case _ => throw new IllegalArgumentException(s"Could not parse data type '${column.dataType}' in column : $name")
    }
    val nullability = if (column.nullable) NULLABLE else NOT_NULLABLE
    new TableDefinition.Column(name, dataType, nullability)
  }

  /**
    * Returns a Tableau Table Definition given a StructType
    */
  private def getTableDefinition(schema: StructType): TableDefinition = {
    val columns = scala.collection.JavaConversions.seqAsJavaList(schema.map(convertStructField))
    new TableDefinition(new TableName(_schema, _table), columns)
  }

  /**
    * Main function for writing a Spark DataFrame to a Tableau Hyper File on disk
    */
  def create(): String = {

    var hyperProcess: Option[HyperProcess] = None
    var connection: Option[Connection] = None

    try {

      hyperProcess = Some(new HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU))

      // Creates a new directory in the default temporary-file directory, using the given prefix to generate its name
      val tempDir: Path = Files.createTempDirectory("hyperleaup")
      val hyperDatabase: Path = Paths.get(s"${tempDir.toString}/${_name}.hyper")
      val csvDir: String = s"${tempDir.toString}/${_name}"
      connection = Some(new Connection(hyperProcess.get.getEndpoint, hyperDatabase.toString, CreateMode.CREATE_AND_REPLACE))

      val catalog: Catalog = connection.get.getCatalog
      catalog.createSchema(new SchemaName(_schema))
      val schema: StructType = _df.schema
      val tableDefinition: TableDefinition = getTableDefinition(schema)
      catalog.createTable(tableDefinition)

      // The most efficient method for adding data to a table is with the COPY command
      _df.coalesce(1).write.option("delimiter", ",").option("header", "true").mode("overwrite").csv(csvDir)
      println(csvDir)
      val csvFilePart = scala.reflect.io.Path(csvDir).toDirectory.files.map(_.name).filter(_.endsWith(".csv")).toSeq.head
      val csvFullPath = s"$csvDir/$csvFilePart"
      val copyCommand = s"COPY ${tableDefinition.getTableName} from '${csvFullPath}' with (format csv, NULL 'NULL', delimiter ',', header)"
      val count = connection.get.executeCommand(copyCommand).getAsLong
      println(s"Copied $count rows.")

      hyperDatabase.toString

    } catch {
      case e: Throwable => throw e
    } finally {
      connection.map(_.close)
      hyperProcess.map(_.close)
    }
  }
}

object Creator {
  def apply(hyperFile: HyperFile): Creator = new Creator(hyperFile)
}
