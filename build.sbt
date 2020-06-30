val sparkCore = "org.apache.spark" %% "spark-core" % "2.4.0"
val sparkSql = "org.apache.spark" %% "spark-sql" % "2.4.0"
val scalatic = "org.scalactic" %% "scalactic" % "3.1.1"
val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1" % "test"

ThisBuild / organization := "com.databricks"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / version := "0.0.1-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
      name := "hyperleaup",
      libraryDependencies ++= Seq(sparkCore, sparkSql, scalatic, scalaTest)
  )

scalacOptions ++= Seq("-Xmax-classfile-name", "78")
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

assemblyJarName := "hyperleaup-0.0.1-uber.jar"

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}
