package mediamath.metrics

import org.scalatest._
import mediamath.metrics._
import com.codahale.metrics._
import com.fasterxml.jackson.module.scala._
import com.fasterxml.jackson.databind.ObjectMapper
import collection._
import scala.collection.immutable.ListMap

class QasinoReporterTestSecure extends FlatSpec with Matchers {

  val qaQasinoServer = "192.168.102.161"
  val userName = "eng"
  val goodPassword = "1qa2ws3e"
  val badPassword = "bogusPassWord"

  import java.util.concurrent.TimeUnit
  
  class StringGauge(value: String) extends Gauge[String] {
    def getValue = value
  }
  class LongGauge(value: Long) extends Gauge[Long] {
    def getValue = value
  }

  val metrics = new MetricRegistry
  val markTime = System.currentTimeMillis
  println(s"Timestamp: $markTime")
  metrics.register("bar.stringGauge", new StringGauge("i'm a little teapot"))
  metrics.register("bar.longGauge", new LongGauge(markTime))
  val counter1 = new Counter
  counter1.inc(100)
  metrics.register("bar.counters.counter1", counter1)
  val counter2 = new Counter
  counter2.inc(200)
  metrics.register("bar.counters.counter2", counter2)
  val counterShouldBeDeleted = new Counter
  counterShouldBeDeleted.inc(100)
  metrics.register("bar.counters.counterShouldBeDeleted", counter1)
  
  it should "authenticate using proper username and password" in {
    val reporter = QasinoReporter.forRegistry(metrics).withHost(qaQasinoServer).withPersist().withGroupings(Set("bar", "bar.counters")).withUsername(userName).withPassword(goodPassword).withSecure().build()
    // Add another counter after registering the metrics
    val counter3 = new Counter
    counter3.inc(300)
    metrics.register("bar.counters.counter3", counter3)
    metrics.remove("bar.counters.counterShouldBeDeleted")
    try {
      reporter.start(1, TimeUnit.SECONDS)
      Thread.sleep(1000)
    }
    finally {
      reporter.shutdown()
    }
  }


  it should "fail trying to connect because a badPassword is used" in {
      val reporter = QasinoReporter.forRegistry(metrics).withHost(qaQasinoServer).withPersist().withGroupings(Set("bar", "bar.counters")).withUsername(userName).withPassword(badPassword).withSecure().build()
          intercept[Throwable] {
              reporter.reportThrowExceptions()
          }
  }




}
