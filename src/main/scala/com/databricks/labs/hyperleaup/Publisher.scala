package com.databricks.labs.hyperleaup

import com.databricks.labs.hyperleaup.utils.SparkSessionWrapper

import scala.xml.XML
import com.databricks.labs.hyperleaup.utils.TableauRestUtils
import org.apache.log4j.Logger

case class LUID(value: String)
case class TableauSignInToken(userId: String, siteId: String, token: String)

class Publisher(hyperFile: HyperFile) extends SparkSessionWrapper {

  // The maximum size of a file that can be published in a single request is 64MB
  private final val FILESIZE_LIMIT_INT = 1024 * 1024 * 64
  // When a Hyper File is over 64MB, break it into 5MB (standard chunk size) chunks
  private final val CHUNK_SIZE_INT = 1024 * 1024 * 5
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
    val token: String = (responseXML \\ "tsResponse" \\ "credentials" \ "@token") text
    val siteId: String = (responseXML \\ "tsResponse" \\ "credentials" \ "site" \ "@id") text
    val userId: String = (responseXML \\ "tsResponse" \\ "credentials" \ "user" \ "@id") text

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
    * Publishes a Tableau Hyper File to a Tableau Server
    */
  def publish(tableauServerUrl: String,
              tableauVersion: String,
              username: String,
              password: String,
              siteContentUrl: String,
              projectName: String,
              dataSourceName: String
             ): LUID = {

    // First, sign-in to Tableau Server
    logger.info("Signing into Tableau Server")
    val signInToken = signIn(tableauServerUrl, tableauVersion, username, password, siteContentUrl)

    // TODO: Actually implement this function
    val datasourceLUID = LUID("1234-1234-1234-1234")

    // Finally, sign-out of Tableau Server
    logger.info("Signing out of Tableau Server")
    signOut(tableauServerUrl, tableauVersion, signInToken.token)

    datasourceLUID
  }

}

object Publisher {
  def apply(hyperFile: HyperFile): Publisher = new Publisher(hyperFile)
}
