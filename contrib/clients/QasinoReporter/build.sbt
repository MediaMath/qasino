name := "QasinoReporter"

version := "1.2.0-SNAPSHOT"

organization := "mediamath"

organizationName := "MediaMath"

organizationHomepage := Some(url("http://mediamath.com"))

scalaVersion := "2.11.2"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

credentials += Credentials("Restricted", "build.datasvc.mediamath.com", "eng", "1qa2ws3e")

externalResolvers := Resolver.withDefaultResolvers(resolvers.value, mavenCentral = false) :+
  ("proxy" at "https://build.datasvc.mediamath.com/artifactory/repo")

publishTo := {
  scala.util.Properties.propIsSet("really_deploy") match {
    case true =>
      if (isSnapshot.value)
        Some("Snapshots" at "https://build.datasvc.mediamath.com/artifactory/snapshots-local")
      else
        Some("Releases" at "https://build.datasvc.mediamath.com/artifactory/releases-local")
    case false => None
  }
}

testOptions in ThisBuild <+= (target in Test) map {
  t => Tests.Argument("-o", "-u", t + "/test-reports")
}

libraryDependencies ++= Seq(
  "mediamath" %% "data-infra-commons" % "1.2.0-SNAPSHOT"
)

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.7",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "ch.qos.logback" % "logback-core" % "1.1.2",
  "io.dropwizard.metrics" % "metrics-core" % "3.1.0",
  "io.dropwizard.metrics" % "metrics-jvm" % "3.1.0",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.4.2",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test"
)
