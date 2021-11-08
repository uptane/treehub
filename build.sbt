name := "treehub"
organization := "io.github.uptane"
scalaVersion := "2.12.15"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

def itFilter(name: String): Boolean = name endsWith "IntegrationSpec"

def unitFilter(name: String): Boolean = !itFilter(name)

lazy val ItTest = config("it").extend(Test)

lazy val UnitTest = config("ut").extend(Test)

lazy val treehub = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .configs(ItTest)
  .settings(inConfig(ItTest)(Defaults.testTasks): _*)
  .configs(UnitTest)
  .settings(inConfig(UnitTest)(Defaults.testTasks): _*)
  .settings(UnitTest / testOptions := Seq(Tests.Filter(unitFilter)))
  .settings(IntegrationTest / testOptions := Seq(Tests.Filter(itFilter)))
  .settings(Seq(libraryDependencies ++= {
    val akkaV = "2.6.17"
    val akkaHttpV = "10.2.7"
    val scalaTestV = "3.0.9"
    val libatsV = "2.0.3"

    Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaV,
      "com.typesafe.akka" %% "akka-stream" % akkaV,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % "test",
      "com.typesafe.akka" %% "akka-http" % akkaHttpV,
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV % "test",
      "com.typesafe.akka" %% "akka-slf4j" % akkaV,
      "org.scalatest"     %% "scalatest" % scalaTestV % "test,it",

      "ch.qos.logback" % "logback-classic" % "1.2.6",
      "org.slf4j" % "slf4j-api" % "1.7.32",

      "io.github.uptane" %% "libats" % libatsV,
      "io.github.uptane" %% "libats-http" % libatsV,
      "io.github.uptane" %% "libats-http-tracing" % libatsV,
      "io.github.uptane" %% "libats-messaging" % libatsV,
      "io.github.uptane" %% "libats-messaging-datatype" % libatsV,
      "io.github.uptane" %% "libats-slick" % libatsV,
      "io.github.uptane" %% "libats-metrics-akka" % libatsV,
      "io.github.uptane" %% "libats-metrics-prometheus" % libatsV,
      "io.github.uptane" %% "libats-logging" % libatsV,
      "io.github.uptane" %% "libats-logging" % libatsV,

      "org.scala-lang.modules" %% "scala-async" % "0.9.6",
      "org.mariadb.jdbc" % "mariadb-java-client" % "2.7.4",

      "com.amazonaws" % "aws-java-sdk-s3" % "1.12.105"
    )
  }))

Compile / mainClass := Some("com.advancedtelematic.treehub.Boot")

import com.typesafe.sbt.packager.docker._

dockerRepository := Some("advancedtelematic")

Docker / packageName := packageName.value

dockerUpdateLatest := true

dockerAliases ++= Seq(dockerAlias.value.withTag(git.gitHeadCommit.value))

Docker / defaultLinuxInstallLocation := s"/opt/${moduleName.value}"

dockerCommands := Seq(
  Cmd("FROM", "advancedtelematic/alpine-jre:adoptopenjdk-jre8u262-b10"),
  ExecCmd("RUN", "mkdir", "-p", s"/var/log/${moduleName.value}"),
  Cmd("ADD", "opt /opt"),
  Cmd("WORKDIR", s"/opt/${moduleName.value}"),
  ExecCmd("ENTRYPOINT", s"/opt/${moduleName.value}/bin/${moduleName.value}"),
  Cmd("RUN", s"chown -R daemon:daemon /opt/${moduleName.value}"),
  Cmd("RUN", s"chown -R daemon:daemon /var/log/${moduleName.value}"),
  Cmd("USER", "daemon")
)

enablePlugins(JavaAppPackaging, GitVersioning, BuildInfoPlugin)

Versioning.settings

buildInfoObject := "AppBuildInfo"
buildInfoPackage := "com.advancedtelematic.treehub"
buildInfoOptions += BuildInfoOption.Traits("com.advancedtelematic.libats.boot.VersionInfoProvider")
buildInfoOptions += BuildInfoOption.ToMap
buildInfoOptions += BuildInfoOption.BuildTime
