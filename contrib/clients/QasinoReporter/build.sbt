name := "QasinoReporter"

organization := "mediamath"

organizationName := "MediaMath"

organizationHomepage := Some(url("http://mediamath.com"))

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "ch.qos.logback" % "logback-core" % "1.0.13",
	"com.codahale.metrics" % "metrics-core" % "3.0.1",
	"net.databinder.dispatch" %% "dispatch-core" % "0.11.0",
	"com.fasterxml.jackson.core" % "jackson-databind" % "2.3.1",
	"org.scalatest" %% "scalatest" % "2.0" % "test",
	"com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.3.1"
)
