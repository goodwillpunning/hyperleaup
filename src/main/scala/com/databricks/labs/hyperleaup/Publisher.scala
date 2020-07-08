package com.databricks.labs.hyperleaup

import java.nio.file.{Files, Paths}

import com.databricks.labs.hyperleaup.utils.SparkSessionWrapper

import scala.xml.XML
import com.databricks.labs.hyperleaup.utils.TableauRestUtils
import org.apache.log4j.Logger

case class LUID(value: String)
case class TableauSignInToken(userId: String, siteId: String, token: String)

class Publisher(hyperFile: HyperFile) extends SparkSessionWrapper {

  // The maximum size of a file that can be published in a single request is 64MB
  private final val MAX_FILESIZE_LIMIT_INT = 1024 * 1024 * 64
  // When a Hyper File is over 64MB, break it into 5MB (standard chunk size) chunks
  // private final val CHUNK_SIZE_INT = 1024 * 1024 * 5
  // Tableau Server releases correspond to an API version
  private final val tableauApiVersions = Map(
    "2019.1" -> "3.3",
    "2019.2" -> "3.4",
    "2019.3" -> "3.5",
    "2019.4" -> "3.6"
  )
  private val logger: Logger = Logger.getLogger(this.getClass)
  private val _path: String = hyperFile.path

  /**
    * Helper function that looks up Tableau API version
    */
  private def getTableauApiVersion(tableauVersion: String): String =  {
    tableauApiVersions(tableauVersion)
  }

  /**
    * Creates a Tableau Server sign-in XML request
    */
  def createSignInXmlRequest(username: String,
                             password: String,
                             siteContentUrl: String
                            ): String = {
    val xml = <tsRequest><credentials name={username} password={password}><site contentUrl={siteContentUrl}/></credentials></tsRequest>
    xml.toString()
  }

  /**
    * Creates a Tableau datasource upload request
    */
  def createUploadXmlRequest(datasourceName: String,
                             projectId: String
                            ): String = {
    val xml = <tsRequest><datasource name={datasourceName}><project id={projectId}/></datasource></tsRequest>
    xml.toString()
  }

  /**
    * Signs into a Tableau Server and returns an auth token
    */
  def signIn(tableauServerUrl: String, tableauServerVersion: String,
             username: String, password: String, siteContentUrl: String): TableauSignInToken = {

    // construct the Tableau Server sign-in URL
    val apiVersion = getTableauApiVersion(tableauServerVersion)
    val url = s"$tableauServerUrl/api/$apiVersion/auth/signin"

    // build the XML sign-in request and send as POST to server
    val request = createSignInXmlRequest(username, password, siteContentUrl)
    val response = TableauRestUtils.post(url, "", request)

    // parse the XML response and get the auth token
    val responseXML = XML.loadString(response)
    val token: String = (responseXML \\ "tsResponse" \\ "credentials" \ "@token").toString
    val siteId: String = (responseXML \\ "tsResponse" \\ "credentials" \ "site" \ "@id").toString
    val userId: String = (responseXML \\ "tsResponse" \\ "credentials" \ "user" \ "@id").toString

    TableauSignInToken(userId, siteId, token)
  }

  /**
    * Signs out of a Tableau Server
    */
  def signOut(tableauServerUrl: String,
              tableauServerVersion: String,
              token: String): Unit = {

    // Construct sign-out URL
    val apiVersion = getTableauApiVersion(tableauServerVersion)
    val url = s"$tableauServerUrl/api/$apiVersion/auth/signout"

    // Send sign-out request with API Token to Tableau Server
    TableauRestUtils.post(url, token, "")
  }

  /**
    * Returns the project ID for the given project name on the Tableau server
    */
  def getProjectId(tableauServerUrl: String,
                   tableauServerVersion: String,
                   token: String,
                   siteId: String,
                   projectName: String): String = {

    // Build the project query request
    val pageNum = 1
    val pageSize = 100
    val apiVersion = getTableauApiVersion(tableauServerVersion)
    val url = s"$tableauServerUrl/api/$apiVersion/sites/$siteId/projects"
    val pagedUrl = url + s"?pageSize=$pageSize&pageNumber=$pageNum"

    // Send a request for the first page of Tableau projects
    val response = TableauRestUtils.get(pagedUrl, token)
    val responseXML = XML.loadString(response)

    // The first requests will yield total num of projects on server
    val numProjects = (responseXML \\ "tsResponse" \\ "pagination" \ "@totalAvailable").toString
    val maxNumPages = (numProjects.toDouble / pageSize).ceil.toInt
    val projects = responseXML \ "projects" \ "project"

    // Next, search through first page of projects for the target project
    var projectId = ""
    if (projects.exists(p => p.attribute("name").getOrElse("").toString == projectName)) {
      // get the project id from matched Tableau project
      val targetProject = projects.filter(_.attribute("name").getOrElse("").toString == projectName).head
      projectId = targetProject.attribute("id").getOrElse("").toString
    } else {

      // if there are more pages to search, make another request
      var currentPage = 2
      while (projectId == "" && currentPage <= maxNumPages) {

        // get the next page
        val nextPageUrl = url + s"?pageSize=$pageSize&pageNumber=$currentPage"
        val response = TableauRestUtils.get(nextPageUrl, token)
        val responseXML = XML.loadString(response)
        val projects = responseXML \ "projects" \ "project"

        if (projects.exists(p => p.attribute("name").getOrElse("").toString == projectName)) {
          // get the project id from matched Tableau project
          val targetProject = projects.filter(_.attribute("name").getOrElse("").toString == projectName).head
          projectId = targetProject.attribute("id").getOrElse("").toString
        }

        currentPage += 1
      }
    }

    // If the project was never found, throw an Exception
    if (projectId == "")
      throw new IllegalArgumentException(s"Project named '$projectName' was not found on the Tableau server")

    projectId
  }

  /**
    * Prepares the Tableau server to receive a chunked Hyper File.
    * Returns the Tableau server upload session ID.
    */
  def startUploadSession(tableauServerUrl: String,
                         tableauServerVersion: String,
                         token: String,
                         siteId: String): String = {

    // Send an upload request to Tableau server
    val apiVersion = getTableauApiVersion(tableauServerVersion)
    val url = s"$tableauServerUrl/api/$apiVersion/sites/$siteId/fileUploads"
    val response = TableauRestUtils.post(url, token, "")

    // Parse the upload ID from the response
    val responseXML = XML.loadString(response)
    (responseXML \\ "tsResponse" \\ "fileUpload" \ "@uploadSessionId").toString
  }


  /**
    * Publishes a Tableau Hyper File to a Tableau Server
    */
  def publish(tableauServerUrl: String,
              tableauVersion: String,
              username: String,
              password: String,
              siteContentUrl: String,
              projectName: String,
              datasourceName: String
             ): LUID = {

    // First, sign-in to Tableau Server
    logger.info("Signing into Tableau Server")
    val signInToken = signIn(tableauServerUrl, tableauVersion, username, password, siteContentUrl)

    // Next, fetch the project ID
    logger.info("Getting Project ID")
    val token = signInToken.token
    val siteId = signInToken.siteId
    val projectId = getProjectId(tableauServerUrl, tableauVersion,
                                 token, siteId, projectName)
    logger.info(s"Project ID: $projectId")

    // Check the size of the Hyper file
    val hyperFileSize = new java.io.File(_path).length
    logger.info(s"The size of the Hyper file is: $hyperFileSize bytes")
    val largeUpload =  hyperFileSize > MAX_FILESIZE_LIMIT_INT


    val uploadResponse = if (largeUpload) {

      TableauRestUtils.sendMultiPartUpload()

    } else {

      // Load the Hyper file from disk
      val file: Array[Byte] = Files.readAllBytes(Paths.get(_path))

      // Build the upload URL
      val apiVersion = getTableauApiVersion(tableauVersion)
      val url = s"$tableauServerUrl/api/$apiVersion/sites/$siteId/datasources?datasourceType=hyper&overwrite=true"
      val uploadRequest: String = createUploadXmlRequest(datasourceName, projectId)

      // Since the file is small, send in a single upload request
      TableauRestUtils.sendSinglePartUpload(
        url, token, siteId, datasourceName,
        projectId, uploadRequest, file
      )
    }

    val responseXML = XML.loadString(uploadResponse)
    val luid = LUID((responseXML \\ "tsResponse" \ "datasource" \ "@luid").toString)

    // Finally, sign-out of Tableau Server
    logger.info("Signing out of Tableau Server")
    signOut(tableauServerUrl, tableauVersion, signInToken.token)

    luid

  }

}

object Publisher {
  def apply(hyperFile: HyperFile): Publisher = new Publisher(hyperFile)
}
