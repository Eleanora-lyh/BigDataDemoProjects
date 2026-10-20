ThisBuild / organization := "com.example"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "1.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-line-count",

    // ========== 依赖 ==========
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.9",
      "org.apache.spark" %% "spark-sql"  % "3.5.9"
    ),
    Compile / run / fork := true
  )