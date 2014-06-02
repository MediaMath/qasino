/*
 * Copyright (C) 2014 MediaMath, Inc. <http://www.mediamath.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.codahale.metrics.jvm.GarbageCollectorMetricSet
import com.codahale.metrics.jvm.MemoryUsageGaugeSet
import com.codahale.metrics.jvm.ThreadStatesGaugeSet
import com.codahale.metrics.{Gauge, Timer, MetricRegistry, Meter}
import mediamath.metrics.QasinoReporter
val registry = new MetricRegistry
registry.register("jvm.memory", new MemoryUsageGaugeSet())
registry.register("jvm.thread", new ThreadStatesGaugeSet())
registry.register("jvm.gc", new GarbageCollectorMetricSet())
val reporter = QasinoReporter.forRegistry(registry)
  .withVerbosity(0)
  .build()
val reporter2 = QasinoReporter.forRegistry(registry)
  .withGroupings(Set("test", "jvm"))
  .withVerbosity(0).build()
registry.register("test.1foo", new Meter())
registry.register("test.bar", new Timer())
registry.register("test.bar.two", new Timer())
registry.register("test.baz", new Gauge[Double](){
  override def getValue: Double = 23.45
})
reporter2.report()































































