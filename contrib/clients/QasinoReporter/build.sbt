name := "QasinoReporter"

organization := "mediamath"

organizationName := "MediaMath"

organizationHomepage := Some(url("http://mediamath.com"))

scalaVersion := "2.10.3"

credentials += Credentials("Restricted", "build.datasvc.mediamath.com", "eng", "1qa2ws3e")

externalResolvers := Resolver.withDefaultResolvers(resolvers.value, mavenCentral = false) :+
  ("proxy" at "https://build.datasvc.mediamath.com/artifactory/repo")

publishTo := {
  scala.util.Properties.propIsSet("deploy_snapshot") match {
    case true => Some("Snapshots" at "https://build.datasvc.mediamath.com/artifactory/snapshots-local")
    case false => None
  }
}

testOptions in ThisBuild <+= (target in Test) map {
  t => Tests.Argument("-o", "-u", t + "/test-reports")
}

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "ch.qos.logback" % "logback-core" % "1.0.13",
  "com.codahale.metrics" % "metrics-core" % "3.0.1",
  "com.codahale.metrics" % "metrics-jvm" % "3.0.1",
  "mediamath" %% "data-infra-commons" % "0.1-SNAPSHOT",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.3.1",
  "org.scalatest" %% "scalatest" % "2.0" % "test",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.3.1"
)

