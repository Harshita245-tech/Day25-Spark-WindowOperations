ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day25-spark",
    version := "1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "3.5.3"
    )
  )
