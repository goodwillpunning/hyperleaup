package com.databricks.labs.hyperleaup.utils

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{PostMethod, StringRequestEntity}

object TableauRestUtils {

  /**
    * Sends an HTTP Post request to Tableau Server.
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
    val status = client.executeMethod(postRequest)

    // return the response
    postRequest.getResponseBodyAsString
  }

}
