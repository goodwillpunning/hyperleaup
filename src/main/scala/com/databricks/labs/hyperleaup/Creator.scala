package com.databricks.labs.hyperleaup

import java.io.File

import com.databricks.labs.hyperleaup.utils.SparkSessionWrapper
import com.tableau.hyperapi.{Catalog, Connection, CreateMode, HyperProcess, SchemaName, SqlType, TableDefinition, TableName, Telemetry}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import com.tableau.hyperapi.Nullability.{NOT_NULLABLE, NULLABLE}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import org.apache.log4j.Logger


class Creator(hyperFile: HyperFile) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  private val _name: String = hyperFile.name
  private val _schema: String = hyperFile.schema
  private val _table: String = hyperFile.table
  private val _path: String = hyperFile.path
  private val _df: DataFrame = hyperFile.getDf

  /**
    * Converts a Spark StructField data type to corresponding Tableau sql type
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
      case _ =>
        // check if the data type is a Decimal(precision,scale)
        if (column.dataType.toString.toLowerCase.startsWith("decimal")) {
          val decimalType = column.dataType.asInstanceOf[DecimalType]
          // Tableau SqlType.numeric has a maximum precision of 18
          val precision = if (decimalType.precision > 18) 18 else decimalType.precision
          val scale = decimalType.scale
          SqlType.numeric(precision, scale)
        } else {
          throw new IllegalArgumentException(s"Could not parse data type '${column.dataType}' in column : $name")
        }
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
    * Checks if Databricks File System environment
    */
  private def isDbfsEnabled: Boolean = {
    spark.conf.get("spark.app.name").contains("Databricks") && Files.exists(Paths.get("/dbfs"))
  }

  /**
    * Writes a Spark DataFrame to a single CSV file on the local
    * filesystem.
    */
  private def writeCsvToLocalFilesystem(df: DataFrame): String = {
    // create a temp dir, prefixed with `hyperleaup`
    val tempDbfsDir: Path = Files.createTempDirectory("hyperleaup")
    val csvDbfsDir: String = tempDbfsDir.toString + "/csv"
    // write the DataFrame to local disk as a single CSV file
    df.coalesce(1).write.option("delimiter", ",").option("header", "true").mode("overwrite").csv(csvDbfsDir)
    // Spark DataFrameWriter will write metadata alongside the CSV,
    // ignore metedata and return only the CSV filename
    val sourceCsvDbfsFiles = new File(csvDbfsDir).listFiles.filter(_.isFile).filter(_.getName.endsWith("csv")).toList
    assert(sourceCsvDbfsFiles.length == 1) // should _always_ return length of 1
    sourceCsvDbfsFiles.head.toString
  }

  /**
    * Writes a Spark DataFrame to a single CSV file and moves the file
    * from the DBFS to a temp dir on the driver node, so that
    * the Tableau Hyper API can execute a COPY operation.
    * COPY operation is, by far, the most *efficient* method for creating a
    * Hyper file.
    */
  private def writeCsvToDbfs(df: DataFrame): String = {
    // create a temp dir, prefixed with `hyperleaup`
    val tempDbfsDir: Path = Files.createTempDirectory("hyperleaup")
    val csvDbfsDir: String = tempDbfsDir.toString + "/csv"
    // write the DataFrame to DBFS as a single CSV file
    df.coalesce(1).write.option("delimiter", ",").option("header", "true").mode("overwrite").csv(csvDbfsDir)
    // move the CSV file to a temp dir on the driver node
    moveCsvToDriverNode("/dbfs" + csvDbfsDir)
  }


  /**
    * Moves a CSV written to a Databricks Filesystem to temp
    * directory on the driver node.
    */
  private def moveCsvToDriverNode(dbfsPath: String): String = {
    // filter the directory contents at DBFS location to only CSV files
    val sourceCsvDbfsFiles = new File(dbfsPath).listFiles.filter(_.isFile).filter(_.getName.endsWith("csv")).toList
    assert(sourceCsvDbfsFiles.length == 1) // should _always_ return length of 1
    val sourceCsvDbfsPath: String = sourceCsvDbfsFiles.head.toString
    val destDriverNodePath: String = getDriverNodeTempDir(sourceCsvDbfsPath)
    // move CSV from DBFS location to temp dir on driver node
    Files.move(
      Paths.get(sourceCsvDbfsPath),
      Paths.get(destDriverNodePath),
      StandardCopyOption.REPLACE_EXISTING
    ).toString
  }

  /**
    * Generates a temp directory on the driver node taking the form:
    * "/tmp/hyperleaup${random_number}/csv/part-${uuid}.csv}"
    */
  private def getDriverNodeTempDir(sourcePath: String): String = {
    val parts = sourcePath.split("/")
    val len = parts.length
    val tempDirPath = s"/tmp/${parts(len-3)}/csv"
    val tempDir = new File(tempDirPath)
    tempDir.mkdirs()
    s"$tempDirPath/${parts(len-1)}"
  }

  /**
    * Creates a directory for the Hyper database given
    * a CSV location.
    * The Hyper database will be created next to `csv` dir.
    */
  def getHyperDatabasePath(targetCsvPath: String): String = {
    val parts = targetCsvPath.split("/")
    val len = parts.length
    val csvFilename = parts(len-1)
    // place Hyper database in `database` dir
    val destDatabasePath = targetCsvPath.replaceAll(s"csv/$csvFilename", "") + "database"
    val destDatabaseDir = new File(destDatabasePath)
    destDatabaseDir.mkdir()
    destDatabasePath
  }

  /**
    * Main function for writing a Spark DataFrame to a Tableau Hyper File on disk
    */
  def create(): String = {

    var hyperProcess: Option[HyperProcess] = None
    var connection: Option[Connection] = None

    try {

      hyperProcess = Some(new HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU))

      // Check if Databricks Filesytem is present
      val csvPath = if (isDbfsEnabled) {
        logger.info("DBFS _IS_ enabled.")
        // A Hyper database cannot be opened using cloud storage,
        // so it must be created on the driver node
        writeCsvToDbfs(_df)
      } else {
        logger.info("DBFS is _NOT_ enabled.")
        writeCsvToLocalFilesystem(_df)
      }

      val hyperDatabasePath = getHyperDatabasePath(csvPath)
      val hyperDatabase: Path = Paths.get(s"$hyperDatabasePath/${_name}.hyper")

      connection = Some(new Connection(hyperProcess.get.getEndpoint, hyperDatabase.toString, CreateMode.CREATE_AND_REPLACE))

      val catalog: Catalog = connection.get.getCatalog
      catalog.createSchema(new SchemaName(_schema))
      val schema: StructType = _df.schema
      val tableDefinition: TableDefinition = getTableDefinition(schema)
      catalog.createTable(tableDefinition)

      // The most efficient method for adding data to a table is with the COPY command
      val copyCommand = s"COPY ${tableDefinition.getTableName} from '$csvPath' with (format csv, NULL 'NULL', delimiter ',', header)"
      val count = connection.get.executeCommand(copyCommand).getAsLong
      logger.info(s"Copied $count rows.")

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
