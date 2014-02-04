name := "QasinoReporter"

version := "1.0"

libraryDependencies ++= Seq(
	"com.codahale.metrics" % "metrics-core" % "3.0.1",
	"net.databinder.dispatch" %% "dispatch-core" % "0.11.0",
	"org.slf4j" % "slf4j-simple" % "1.7.5",
	"com.fasterxml.jackson.core" % "jackson-databind" % "2.3.1",
	"org.scalatest" %% "scalatest" % "2.0" % "test",
	"com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.3.1",
	"org.mockito" % "mockito-all" % "1.9.5"
)
