package com.databricks.labs.hyperleaup

import com.databricks.labs.hyperleaup.utils.SparkSessionWrapper


case class LUID(value: String)

class Publisher(hyperFile: HyperFile) extends SparkSessionWrapper {

  private val _path: String = hyperFile.path

  /**
    * Publishes a Tableau Hyper File to a Tableau Server
    */
  def publish(tableauServer: String,
              tableauVersion: String,
              username: String,
              password: String,
              siteContentUrl: String,
              projectName: String,
              dataSourceName: String
             ): LUID = {
    // TODO: Actually implement this function
    LUID("1234-1234-1234-1234")
  }

}

object Publisher {
  def apply(hyperFile: HyperFile): Publisher = new Publisher(hyperFile)
}
