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

package mediamath.metrics

import com.codahale.metrics._
import com.fasterxml.jackson.databind.ObjectMapper
import dispatch._
import java.util.concurrent.{Executors, TimeUnit}
import java.net.{Inet4Address, NetworkInterface}
import scala.collection._
import java.util.{SortedMap => JavaSortedMap}
import scala.language.existentials
import scala.collection.immutable.ListMap
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import java.io.IOException
import scala.collection.JavaConversions._
import mediamath.utils.PrefixedEnumeration
import QasinoReporter._

/**
 * Qasino metrics reporter
 */
object QasinoReporter {

  val DEFAULT_HOST = "localhost"
  val DEFAULT_PORT = 15597
  val DEFAULT_SECURE_PORT = 443
  val DEFAULT_NUM_THREADS = 1

  val QASINO_PATH_DEFAULT = "request"
  val QASINO_OP_DEFAULT = "add_table_data"

	val registryNameSeparator = "_"

  /** Remove any instances of the illegal characters from the name */
  def sanitizeString(name: String): String = {
    val sanitary = """[^a-z0-9_]""".r.replaceAllIn(name.toLowerCase, registryNameSeparator)

    // if it starts with a number then prefix with _
    """[0-9]""".r.findPrefixOf(sanitary) match {
      case Some(_) => "_" + sanitary
      case None => sanitary
    }
  }

  /** Get a non-loopback ip address for host (if possible) */
	def getFirstNonLoopbackAddress: String = {
		for {
      iface <- NetworkInterface.getNetworkInterfaces.filter(!_.isLoopback)
      addr <- iface.getInetAddresses if addr.isInstanceOf[Inet4Address]
    } {
      return addr.getHostAddress
    }

    "0.0.0.0"
	}

  /** Create a new builder for a given registry */
  def forRegistry(registry: MetricRegistry): Builder = {
    new Builder(registry)
  }

  /**
   * Builder for creating a QasinoReporter
   */
  class Builder(reg: MetricRegistry) {
    private[metrics] var registry: MetricRegistry = reg
    private[metrics] var host: String = DEFAULT_HOST
    private[metrics] var port: Int = -1
    private[metrics] var secure: Boolean = false
    private[metrics] var username: String = null
    private[metrics] var password: String = null
    private[metrics] var path: String = QASINO_PATH_DEFAULT
    private[metrics] var op: String = QASINO_OP_DEFAULT
    private[metrics] var persist: Boolean = false
    private[metrics] var groupings: Set[String] = Set.empty
    private[metrics] var filter: MetricFilter = MetricFilter.ALL
    private[metrics] var numThreads: Int = DEFAULT_NUM_THREADS
    private[metrics] var rateUnit: TimeUnit = TimeUnit.SECONDS
    private[metrics] var durationUnit: TimeUnit = TimeUnit.MILLISECONDS
    private[metrics] var verbosity: Int = 0

    def withPort(port: Int): this.type = {
      this.port = port
      this
    }

    def withHost(host: String): this.type = {
      this.host = host
      this
    }

    def withSecure(secure: Boolean = true): this.type = {
      this.secure = secure
      this
    }

    def withUsername(username: String): this.type = {
      this.username = username
      this
    }

    def withPassword(password: String): this.type = {
      this.password = password
      this
    }

    def withPath(path: String): this.type = {
      this.path = path
      this
    }

    def withOp(db_op: String): this.type = {
      this.op = db_op
      this
    }

    def withPersist(db_persist: Boolean = true): this.type = {
      this.persist = db_persist
      this
    }

    def withGroupings(groupings: Set[String]): this.type = {
      this.groupings ++= groupings
      this
    }

    def withFilter(filter: MetricFilter): this.type = {
      this.filter = filter
      this
    }

    def withNumThreads(numThreads: Int): this.type = {
      this.numThreads = numThreads
      this
    }

    def convertRatesTo(rateUnit: TimeUnit): this.type = {
      this.rateUnit = rateUnit
      this
    }

    def convertDurationsTo(durationUnit: TimeUnit): this.type = {
      this.durationUnit = durationUnit
      this
    }

    def withVerbosity(verbosity:Int): this.type = {
      this.verbosity = verbosity
      this
    }

    def build(): QasinoReporter = {
      if ( secure && (username == null || password == null) ) throw new IllegalArgumentException("username and password are required")

      if ( port <= 0 )
        port = if ( secure ) DEFAULT_SECURE_PORT else DEFAULT_PORT

      new QasinoReporter(this)
    }
  }

  class QasinoReporterException(message:String, cause:Throwable) extends IOException(message, cause)
  class ConnectionException(cause:Throwable) extends QasinoReporterException("Connection Failure", cause)
  class HTTPException(message:String, cause:Throwable) extends QasinoReporterException(message, cause)
  class ClientException(cause:Throwable) extends HTTPException("Client Exception", cause)
  class ServerException(cause:Throwable) extends HTTPException("Server Exception", cause)
  class AuthenticationException(cause:Throwable = null) extends HTTPException("401 Not Authorized", cause)
}

/**
 * Qasino metics reporter
 */
class QasinoReporter(builder: Builder) extends
		ScheduledReporter(
      builder.registry,
      classOf[QasinoReporter].getSimpleName,
      builder.filter,
      builder.rateUnit,
      builder.durationUnit) {
	val registry: MetricRegistry = builder.registry
	val host: String = builder.host
	val port: Int = builder.port
	val secure: Boolean = builder.secure
  val username: String = builder.username
  val password: String = builder.password
	val path: String = builder.path
	val op: String = builder.op
	val persist: Boolean = builder.persist
	val orderedGroupings: Seq[String] = builder.groupings.toSeq.map(sanitizeString).sortWith(_.length >= _.length)
	val filter: MetricFilter = builder.filter
	val rateUnit: TimeUnit = builder.rateUnit
	val durationUnit: TimeUnit = builder.durationUnit
  val numThreads: Int = builder.numThreads
  val verbosity:Int = builder.verbosity

  // Set up logger
  private val log = LoggerFactory.getLogger(this.getClass)

  // used for identifying self to qasino
  private val reporterHost = getFirstNonLoopbackAddress

  // Create the execution context for dispatch to run in
  private implicit val exc: ExecutionContext = new ExecutionContext {
    val threadPool = Executors.newFixedThreadPool(numThreads)

    override def execute(runnable: Runnable): Unit = threadPool.submit(runnable)

    override def reportFailure(t: Throwable): Unit = throw t
  }

  // Set up Dispatch HTTP client
	private val dispatchHost =
    if (secure)
      dispatch.host(host, port).secure
    else
      dispatch.host(host, port)

  // create Dispatch
	private val dispatchRequest =
    if (secure)
      (dispatchHost / path).POST <<? Map("op" -> op) as_!(username, password)
    else
      (dispatchHost / path).POST <<? Map("op" -> op)

	// JSON mapper singleton
	private val mapper = new ObjectMapper()

  // Qasino column names
	private object QasinoRequestIdentifier extends PrefixedEnumeration {
		// Enumeration for all the JSON keys for qasino for safety
		type QasinoRequestIdentifier = PrefixedValue

		val key_op, key_identity, key_tablename, key_table, key_column_names, key_column_types, key_rows, key_persist = PrefixedValue("key_")
	}
	import QasinoRequestIdentifier._

	// Default map for JSON
	private val defaultDataJson = mutable.Map[String, Any](
    s"$key_op" -> op,
    s"$key_identity" -> reporterHost,
    s"$key_persist" -> { if (persist) 1 else 0 },
    s"$key_table" -> mutable.Map[String, Any](
      s"$key_tablename" -> Unit,
      s"$key_column_names" -> Unit,
      s"$key_column_types" -> Unit
		)
	)

  private[metrics] val getGaugeSpec: PartialFunction[Metric, Seq[(String, String, Any)]] = {
    case gauge:Gauge[_] => Seq(("value", "text", gauge.getValue.toString))
    case _ => Seq.empty
  }

  private[metrics] val getCountingSpec: PartialFunction[Metric, Seq[(String, String, Any)]] = {
    case counter:Counting => Seq(("count", "integer", counter.getCount))
    case _ => Seq.empty
  }

  private[metrics] val getSamplingSpec: PartialFunction[Metric, Seq[(String, String, Any)]] = {
    case timer:Timer =>
      val snapshot = timer.getSnapshot

      Seq(
        ("min_time", "real", convertDuration(snapshot.getMin)),
        ("max_time", "real", convertDuration(snapshot.getMax)),
        ("mean_time", "real", convertDuration(snapshot.getMean)),
        ("time_unit", "text", durationUnit.toString)
      ) ++ {
        if (verbosity >= 1)
          Seq(
            ("stddev_time", "real", convertDuration(snapshot.getStdDev)),
            ("p50_time", "real", convertDuration(snapshot.getMedian)),
            ("p75_time", "real", convertDuration(snapshot.get75thPercentile)),
            ("p95_time", "real", convertDuration(snapshot.get95thPercentile)),
            ("p98_time", "real", convertDuration(snapshot.get98thPercentile)),
            ("p99_time", "real", convertDuration(snapshot.get99thPercentile)),
            ("p999_time", "real", convertDuration(snapshot.get999thPercentile))
          )
        else
          Seq.empty
      }
    case sampler:Sampling =>
      val snapshot = sampler.getSnapshot

      Seq(
        ("min", "integer", snapshot.getMin),
        ("max", "integer", snapshot.getMax),
        ("mean", "real", snapshot.getMean)
      ) ++ {
        if (verbosity >= 1)
          Seq(
            ("stddev", "real", snapshot.getStdDev),
            ("p50", "real", snapshot.getMedian),
            ("p75", "real", snapshot.get75thPercentile),
            ("p95", "real", snapshot.get95thPercentile),
            ("p98", "real", snapshot.get98thPercentile),
            ("p99", "real", snapshot.get99thPercentile),
            ("p999", "real", snapshot.get999thPercentile)
          )
        else
          Seq.empty
      }
    case _ => Seq.empty
  }

  private[metrics]  val getMeteredSpec: PartialFunction[Metric, Seq[(String, String, Any)]] = {
    case meter:Metered =>
      Seq(
        ("mean_rate", "real", convertRate(meter.getMeanRate)),
        ("rate_unit", "text", rateUnit.toString)
      ) ++ {
        if (verbosity >= 1)
          Seq(
            ("m1_rate", "real", convertRate(meter.getOneMinuteRate)),
            ("m5_rate", "real", convertRate(meter.getFiveMinuteRate)),
            ("m15_rate", "real", convertRate(meter.getFifteenMinuteRate))
          )
        else
          Seq.empty
      }
    case _ => Seq.empty
  }

  private[metrics]  def getSpec(metric:Metric, prefixOption:Option[String] = None): (Seq[String], Seq[String], Seq[Any]) = {
    val spec =
      getGaugeSpec(metric) ++
      getCountingSpec(metric) ++
      getSamplingSpec(metric) ++
      getMeteredSpec(metric)

    (prefixOption match {
      case Some(prefix) => spec map { case (n, t, v) => (prefix + registryNameSeparator + n, t, v) }
      case None => spec
    }).unzip3
  }

  private[metrics] def createJson(tableName:String, names:Seq[String], types:Seq[String], values:Seq[Any]): String = {
    val tableMap = mutable.Map[String, Any](
      s"$key_tablename" -> tableName,
      s"$key_column_names" -> seqAsJavaList("host" +: names),
      s"$key_column_types" -> seqAsJavaList("text" +: types),
      s"$key_rows" -> java.util.Arrays.asList(seqAsJavaList(reporterHost +: values))
    )

    mapper.writeValueAsString(mapAsJavaMap(defaultDataJson + (s"$key_table" -> mapAsJavaMap(tableMap))))
  }

  /** get JSON for a metric or group */
	private[metrics] def getJson(nameToMetric: ListMap[String, Metric]) = {
    var groups: Map[Option[String], Map[Option[String], Metric]] = Map.empty

    for ( (unsanitaryName, metric) <- nameToMetric ) {
      val metricName = sanitizeString(unsanitaryName)
      val groupName = orderedGroupings.find( group => metricName == group || metricName.startsWith(group + registryNameSeparator) )

      val subGroupName = groupName match {
        case Some(group) =>
          if ( metricName == group )
            None
          else
            Some(sanitizeString(metricName.drop(group.length + 1)))
        case None =>
          Some(sanitizeString(metricName))
      }

      groups += (groupName -> (groups.getOrElse(groupName, Map.empty[Option[String], Metric]) + (subGroupName -> metric)))
    }

    var jsonSeq = Seq.empty[String]

    for {
      groupName <- groups.keys.toSeq.sorted
      metricMap = groups(groupName)
    } {
      groupName match {
        case Some(tableName) =>
          var combinedNames = Seq.empty[String]
          var combinedTypes = Seq.empty[String]
          var combinedValues = Seq.empty[Any]

          for {
            prefix <- metricMap.keys.toSeq.sorted
            (names, types, values) = getSpec(metricMap(prefix), prefix)
          } yield {
            combinedNames ++= names
            combinedTypes ++= types
            combinedValues ++= values
          }

          jsonSeq :+= createJson(tableName, combinedNames, combinedTypes, combinedValues)
        case None =>
          for {
            (tableName, metric) <- metricMap
            (names, types, values) = getSpec(metric, None)
          } {
            jsonSeq :+= createJson(tableName.get, names, types, values)
          }
      }
    }

    jsonSeq
  }

  /** send latest metrics to Qasino */
  private def reportToQasino(nameToMetric: ListMap[String, Metric]): Unit = {
    // send a request for each table update
    val futures =
      getJson(nameToMetric) map { json =>
        if ( log.isDebugEnabled )
          log.debug(s"Qasino request JSON: $json")

        val postWithParams = dispatchRequest << json
        dispatch.Http(postWithParams OK as.String)
      }

    // block until all updates have responded
    try {
      try {
        Await.result(Future.sequence(futures), scala.concurrent.duration.Duration.Inf) // Block until all futures are resolved})
      } catch {
        case ex:java.util.concurrent.ExecutionException => throw ex.getCause
      }
    } catch {
      case ex:dispatch.StatusCode =>
        if ( ex.code == 401 ) throw new AuthenticationException(ex)
        if ( ex.code / 100 == 4 ) throw new ClientException(ex)
        if ( ex.code / 100 == 5 ) throw new ServerException(ex)
        throw new HTTPException("Unexpected HTTP response", ex)
      case ex:java.net.ConnectException => throw new ConnectionException(ex)
    }
	}

  /** Create a map of all metrics */
	private[metrics] def combineMetricsToMap(
		 gauges: JavaSortedMap[String, Gauge[_]] = registry.getGauges,
		 counters: JavaSortedMap[String, Counter] = registry.getCounters,
		 histograms: JavaSortedMap[String, Histogram] = registry.getHistograms,
		 meters: JavaSortedMap[String, Meter] = registry.getMeters,
		 timers: JavaSortedMap[String, Timer] = registry.getTimers): ListMap[String, Metric] = {
		ListMap(gauges.toSeq: _*) ++
		ListMap(counters.toSeq: _*) ++
		ListMap(histograms.toSeq: _*) ++
		ListMap(meters.toSeq: _*) ++
		ListMap(timers.toSeq: _*)
	}

  /** report on metrics */
  override def report (
      gauges: JavaSortedMap[String, Gauge[_]],
      counters: JavaSortedMap[String, Counter],
      histograms: JavaSortedMap[String, Histogram],
      meters: JavaSortedMap[String, Meter],
      timers: JavaSortedMap[String, Timer]) {

    try {
      reportThrowExceptions(gauges, counters, histograms, meters, timers)
    } catch {
      case ex:IllegalArgumentException => throw ex
      case ex:Throwable =>
        log.error("Failed to report data", ex)
    }
  }

  /** report implementation that throws exceptions */
  private[metrics] def reportThrowExceptions (
			gauges: JavaSortedMap[String, Gauge[_]],
			counters: JavaSortedMap[String, Counter],
			histograms: JavaSortedMap[String, Histogram],
			meters: JavaSortedMap[String, Meter],
			timers: JavaSortedMap[String, Timer]) {

    reportToQasino(combineMetricsToMap(
      gauges,
      counters,
      histograms,
      meters,
      timers
    ))
	}

  /** report implementation that throws exceptions */
  private[metrics] def reportThrowExceptions():Unit = {
    reportThrowExceptions(
      registry.getGauges(filter),
      registry.getCounters(filter),
      registry.getHistograms(filter),
      registry.getMeters(filter),
      registry.getTimers(filter))
  }

  /** stop scheduled reporting */
  def shutdown(): Unit = {
    stop() // Stop the timer
    report() // Flush out the remaining data
  }
}
