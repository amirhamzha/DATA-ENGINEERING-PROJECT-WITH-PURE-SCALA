val scala3Version = "3.7.4"

lazy val root = project
  .in(file("."))
  .settings(
    name := "internship_project",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.0.0" % Test,
      "org.quartz-scheduler" % "quartz" % "2.3.2"
    ),
    
    // Assembly plugin settings
    assembly / assemblyJarName := "internship-pipeline.jar",
    assembly / mainClass := Some("app.MainCli"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
