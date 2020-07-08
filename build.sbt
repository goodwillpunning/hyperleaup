val sparkCore = "org.apache.spark" %% "spark-core" % "2.4.0"
val sparkSql = "org.apache.spark" %% "spark-sql" % "2.4.0"
val scalatic = "org.scalactic" %% "scalactic" % "3.1.1"
val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1" % "test"
val httpClient = "org.apache.httpcomponents" % "httpclient" % "4.5.12"
val httpMime = "org.apache.httpcomponents" % "httpmime" % "4.5.12"

ThisBuild / organization := "com.databricks"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / version := "0.0.1-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
      name := "hyperleaup",
      libraryDependencies ++= Seq(sparkCore, sparkSql, scalatic, scalaTest, httpClient, httpMime)
  )

scalacOptions ++= Seq("-Xmax-classfile-name", "78")
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

