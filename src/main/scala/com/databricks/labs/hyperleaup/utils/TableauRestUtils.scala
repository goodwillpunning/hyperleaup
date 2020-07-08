package com.databricks.labs.hyperleaup.utils

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{GetMethod, PostMethod, StringRequestEntity}
import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.content.{ByteArrayBody, StringBody}
import org.apache.http.entity.mime.{FormBodyPartBuilder, MultipartEntityBuilder}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils

object TableauRestUtils {

  /**
    * Sends an HTTP POST request to Tableau Server.
    * Returns the response body as a String.
    */
  def post(url: String, token: String, payload: String): String = {

    val client = new HttpClient()
    val postRequest = new PostMethod(url)

    // add auth token
    if (token != "") {
      postRequest.setRequestHeader("x-tableau-auth", token)
    }

    // populate the request with headers + message body
    if (payload != "") {
      postRequest.setRequestHeader("Content-Language", "en-US")
      postRequest.setRequestEntity(new StringRequestEntity(payload, "text/xml", "ISO-8859-1"))
    }

    // execute and retrieve HTTP status
    client.executeMethod(postRequest)

    // return the response
    postRequest.getResponseBodyAsString
  }

  /**
    * Sends an HTTP GET request to Tableau Server.
    * Returns the response body as a String.
    */
  def get(url: String, token: String): String = {

    val client = new HttpClient()
    val getRequest = new GetMethod(url)

    // add the auth token in the request header
    getRequest.setRequestHeader("x-tableau-auth", token)

    // execute and retrieve HTTP status
    client.executeMethod(getRequest)

    // return the response
    getRequest.getResponseBodyAsString
  }

  /**
    * Sends multi-part file upload to Tableau Server
    */
  def sendMultiPartUpload(): String = {
    throw new UnsupportedOperationException(s"Uploading Hyper files over 64 MB is not supported.")
  }

  /**
    * Uploads Hyper file as a single request to Tableau Server
    */
  def sendSinglePartUpload(url: String,
                           token: String,
                           siteId: String,
                           datasourceName: String,
                           projectId: String,
                           uploadRequestXml: String,
                           file: Array[Byte]
                          ): String = {

    // Construct the upload request
    val post: HttpPost = new HttpPost(url)

    // Add the auth token
    post.setHeader("x-tableau-auth", token)

    // Request will include 2 parts: upload request as XML and the file as a binary object
    val entity: HttpEntity = MultipartEntityBuilder.create()
      .setMimeSubtype("mixed")
      .addPart(FormBodyPartBuilder.create()
        .setName("request_payload")
        .setBody(new StringBody(uploadRequestXml, ContentType.TEXT_XML))
        .build())
      .addPart(FormBodyPartBuilder.create()
        .setName("tableau_datasource")
        .setBody(new ByteArrayBody(file, datasourceName))
        .build())
      .build()
    post.setEntity(entity)

    // Send upload request to Tableau server
    val client: CloseableHttpClient = HttpClientBuilder.create().build()
    val response: CloseableHttpResponse = client.execute(post)

    EntityUtils.toString(response.getEntity)
  }


}
