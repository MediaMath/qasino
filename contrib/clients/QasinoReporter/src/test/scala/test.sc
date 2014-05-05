import com.codahale.metrics.{Gauge, Timer, MetricRegistry, Meter}
import mediamath.metrics.QasinoReporter
val registry = new MetricRegistry
val reporter = QasinoReporter.forRegistry(registry)
  .withVerbosity(0)
  .build()
val reporter2 = QasinoReporter.forRegistry(registry)
  .withGroupings(Set("test", "test.bar"))
  .withVerbosity(0).build()
registry.register("test.1foo", new Meter())
registry.register("test.bar", new Timer())
registry.register("test.bar.two", new Timer())
registry.register("test.baz", new Gauge[Double](){
  override def getValue: Double = 23.45
})
reporter2.report()









































