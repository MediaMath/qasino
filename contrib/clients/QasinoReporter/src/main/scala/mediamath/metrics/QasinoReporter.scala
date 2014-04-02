package mediamath.metrics

import com.codahale.metrics._
import com.fasterxml.jackson.databind.ObjectMapper
import dispatch._
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection
import java.net.{Inet4Address, NetworkInterface}
import collection._
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
	val illegalCharRegex = new scala.util.matching.Regex("""[^A-Za-z0-9_]""")

  /** Remove any instances of the illegal characters from the name */
  def sanitizeString(name: String): String =
		illegalCharRegex.replaceAllIn(name.toLowerCase, registryNameSeparator)

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
    private[metrics] var groupings: Set[String] = SortedSet.empty
    private[metrics] var filter: MetricFilter = MetricFilter.ALL
    private[metrics] var numThreads: Int = DEFAULT_NUM_THREADS
    private[metrics] var rateUnit: TimeUnit = TimeUnit.SECONDS
    private[metrics] var durationUnit: TimeUnit = TimeUnit.MILLISECONDS

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
      this.groupings = groupings.map {sanitizeString}
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
	val groupings: Set[String] = builder.groupings
	val filter: MetricFilter = builder.filter
	val rateUnit: TimeUnit = builder.rateUnit
	val durationUnit: TimeUnit = builder.durationUnit
  val numThreads: Int = builder.numThreads

  // Set up logger
  private val log = LoggerFactory.getLogger(this.getClass)

  // used for identifing self to qasino
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

  // create a copy of the registry with names as valid qasino columns
  private[metrics] def sanitizeRegistry(registry: MetricRegistry): MetricRegistry = {
    // Return a new MetricRegistry with names sanitized for qasino
    val sanitizedRegistry = new MetricRegistry
    val metricMap = mapAsScalaMap(registry.getMetrics)
    for ((name, metric) <- metricMap) {
      sanitizedRegistry.register(sanitizeString(name), metric)
    }

    if (registryHasCollisions(sanitizedRegistry))
      throw new IllegalArgumentException("Found a collision within registry names after sanitation")

    if (hasIllegalColumnNames(sanitizedRegistry))
      throw new IllegalArgumentException("Found a column beginning with a non-alpha character")

    sanitizedRegistry
  }

  // check for namespace collisions caused by sanitization
  private[metrics] def registryHasCollisions(registry: MetricRegistry): Boolean = {
    // Check whether we have any name collisions after some sanitizing
    val namesSet = mutable.Set[String]()
    val registryNames = registry.getNames
    for (name <- asScalaSet(registryNames)) {
      val sanitizedName = sanitizeString(name)
      namesSet.add(sanitizedName)
    }
    namesSet.size < registryNames.size()
  }

  // Check whether illegal column names would be generated by the current registry
  private[metrics] def hasIllegalColumnNames(registry: MetricRegistry): Boolean = {
    var hasIllegalColName = false
    for (name <- registry.getNames if !hasIllegalColName) {
      val thisGrouping: Option[String] =
        groupings.toSeq.sortBy(_.length).reverse.find(s => name.startsWith(s + "_"))
      val suffix: String = if (thisGrouping.isDefined) {
        name.drop(thisGrouping.get.length + 1)
      }
      else name
      hasIllegalColName = suffix.matches("^[^A-Za-z].*")
    }
    hasIllegalColName
  }

	// Shorthand for a two dimensional map of any type
	private type TwoDMap[K1, K2, Val] = ListMap[K1, ListMap[K2, Val]]

  /** map metric fields to column names */
  private def getColumnNames(metric: Metric, prefixWithSeparator: String = ""): Seq[String] = metric match {
		// Get the qasino column names for any metric type
		case gauge: Gauge[_] =>
			Seq(
        "value"
      ) map {prefixWithSeparator + _}
		case _: Counter =>
			Seq(
        "count"
      ) map {prefixWithSeparator + _}
		case _: Histogram =>
			Seq(
        "count",
        "max",
        "mean",
        "mid",
        "stddev",
        "p50",
        "p75",
        "p95",
        "p98",
        "p99",
        "p999"
      ) map {prefixWithSeparator + _}
		case _: Meter =>
			Seq(
        "count",
        "mean_rate",
        "m1_rate",
        "m5_rate",
        "m15_rate",
        "rate_unit"
      ) map {prefixWithSeparator + _}
		case _: Timer =>
			Seq(
        "count",
        "max",
        "mean",
        "min",
        "stddev",
        "p50",
        "p75",
        "p95",
        "p98",
        "p99",
        "p999",
        "mean_rate",
        "m1_rate",
        "m5_rate",
        "m15_rate",
        "rate_unit",
        "duration_unit"
      ) map {prefixWithSeparator + _}
		case _ => Seq.empty[String]
	}

  /** map metric fields to column types */
  private def getColumnTypes(metric: Metric, prefix: String = ""): Seq[String] = metric match {
		// Get the qasino column types for any metric type
    case gauge: Gauge[_] => Seq(
      "text"  // value
    )
		case _: Counter => Seq(
      "integer"  // count
    )
		case _: Histogram => Seq(
      "integer", // count
      "integer", // max
      "real", // mean
      "integer", // min
      "real", // stddev
      "real", // p50
      "real", // p75
      "real", // p95
      "real", // p98
      "real", // p99
      "real"  // p999
    )
		case _: Meter => Seq(
      "integer", // count
      "real", // mean_rate
      "real", // m1_rate
      "real", // m5_rate
      "real", // m15_rate
      "text" // rate_unit
    )
		case _: Timer => Seq(
      "integer", // count
      "real", // max
      "real", // mean
      "real", // min
      "real", // stddev
      "real", // 50p
      "real", // 75p
      "real", // 95p
      "real", // 98p
      "real", // 99p
      "real", // 999p
      "real", // mean_rate
      "real", // m1_rate
      "real", // m5_rate
      "real", // m15_rate
      "text", // rate_unit
      "text"  // duration_unit
    )
		case _ => Seq.empty[String]
	}

  /** map metric fields to column values */
  private def getColumnValues(metric: Metric):Seq[Any] = metric match {
		// Get the qasino column values for any metric type
		case gauge: Gauge[_] => Seq(
      gauge.getValue.toString
    )
		case counter: Counter => Seq(
      counter.getCount
    )
		case histogram: Histogram =>
			val snap = histogram.getSnapshot
      Seq(
        histogram.getCount,
        convertDuration(snap.getMax),
        convertDuration(snap.getMean),
        convertDuration(snap.getMin),
        convertDuration(snap.getStdDev),
        convertDuration(snap.getMedian),
        convertDuration(snap.get75thPercentile),
        convertDuration(snap.get95thPercentile),
        convertDuration(snap.get98thPercentile),
        convertDuration(snap.get99thPercentile),
        convertDuration(snap.get999thPercentile)
      )
		case meter: Meter =>
      Seq(
        meter.getCount,
        convertRate(meter.getMeanRate),
        convertRate(meter.getOneMinuteRate),
        convertRate(meter.getFiveMinuteRate),
        convertRate(meter.getFifteenMinuteRate),
        rateUnit
      )
		case timer: Timer =>
      val snap = timer.getSnapshot
      Seq(
        convertDuration(timer.getCount),
        convertDuration(snap.getMax),
        convertDuration(snap.getMean),
        convertDuration(snap.getMin),
        convertDuration(snap.getStdDev),
        convertDuration(snap.getMedian),
        convertDuration(snap.get75thPercentile),
        convertDuration(snap.get95thPercentile),
        convertDuration(snap.get98thPercentile),
        convertDuration(snap.get99thPercentile),
        convertDuration(snap.get999thPercentile),
        convertRate(timer.getMeanRate),
        convertRate(timer.getOneMinuteRate),
        convertRate(timer.getFiveMinuteRate),
        convertRate(timer.getFifteenMinuteRate),
        rateUnit,
        durationUnit
      )
	}

  /** map metric fields to grouped column types */
  private def getGroupedColumnNames(groupedMetrics: TwoDMap[String, String, Metric], prefix: String): Seq[String] = {
    groupedMetrics.getOrElse(prefix, Map.empty[String, Metric]) flatMap {
      case (suffix, metric) =>
        getColumnNames(metric, suffix + "_")
    }
  }.toSeq

  /** map metric fields to grouped column types */
  private def getGroupedColumnTypes(groupedMetrics: TwoDMap[String, String, Metric], prefix: String): Seq[String] = {
    groupedMetrics.getOrElse(prefix, Map.empty[String, Metric]) flatMap { case (_, metric) =>
      getColumnTypes(metric)
    }
  }.toSeq

  /** map metric fields to grouped column values */
  private def getGroupedColumnValues(groupedMetrics: TwoDMap[String, String, Metric], prefix: String): Seq[Any] = {
    groupedMetrics.getOrElse(prefix, Map.empty[String, Metric]) flatMap { case (_, metric) =>
      getColumnValues(metric)
    }
  }.toSeq

  /** convert metric to JSON */
  private def getJsonForMetric(metric: Metric, name: String): String = {
		// Get the qasino json data for any metric type
		var postDataMap = defaultDataJson
		val col_names  = "host" +: getColumnNames(metric)
		val col_types  = "text" +: getColumnTypes(metric)
		val col_values = reporterHost +: getColumnValues(metric)
		val tableMap = mutable.Map[String, Any](
			s"$key_tablename" -> name,
      s"$key_column_names" -> seqAsJavaList(col_names),
      s"$key_column_types" -> seqAsJavaList(col_types),
      s"$key_rows" -> java.util.Arrays.asList(seqAsJavaList(col_values))
		)
		postDataMap = postDataMap + (s"$key_table" -> mapAsJavaMap(tableMap))
		mapper.writeValueAsString(mapAsJavaMap(postDataMap))
	}

  /** convert grouped metrics to JSON */
  private def getJsonForGroupedMetrics(groupedMetrics: TwoDMap[String, String, Metric], prefix: String): String = {
		// Get the qasino json data for any grouped metric type
		var postDataMap = defaultDataJson
		val col_names = "host" +: getGroupedColumnNames(groupedMetrics, prefix)
		val col_types =  "text" +: getGroupedColumnTypes(groupedMetrics, prefix)
		val col_values = reporterHost +: getGroupedColumnValues(groupedMetrics, prefix)
		val tableMap = mutable.Map[String, Any](
      s"$key_tablename" -> prefix,
      s"$key_column_names" -> seqAsJavaList(col_names),
      s"$key_column_types" -> seqAsJavaList(col_types),
      s"$key_rows" -> java.util.Arrays.asList(seqAsJavaList(col_values))
		)
		postDataMap = postDataMap + (s"$key_table" -> mapAsJavaMap(tableMap))
		mapper.writeValueAsString(mapAsJavaMap(postDataMap))
	}

  /** group metrics by the prefix map */
	private def groupMetrics(metrics: Map[String, Metric]): TwoDMap[String, String, Metric] = {
		var groupedMetrics: TwoDMap[String, String, Metric] = ListMap.empty
		val emptryString = ""
		for ((name, metric) <- metrics) {
			// Match groups going by longest group name to shortest
			val thisGrouping: Option[String] =
				groupings.toSeq.sortBy(_.length).reverse.find(s => name.startsWith(s + "_"))
			val suffix: String = if (thisGrouping.isDefined) {
				// Add one to the length for the separator
				name.drop(thisGrouping.get.length + 1)
			}
			else name
			var subgroupedMetrics: ListMap[String, Metric] = groupedMetrics.getOrElse(thisGrouping.getOrElse(emptryString), ListMap.empty)
			subgroupedMetrics = subgroupedMetrics + (suffix -> metric)
			groupedMetrics = groupedMetrics + (thisGrouping.getOrElse(emptryString) -> subgroupedMetrics)
		}
		groupedMetrics
	}

  /** get JSON for a metric or group */
	private[metrics] def getJson(nameToMetric: ListMap[String, Metric]): Seq[String] = {
		var jsonForMetrics = Seq.empty[String]
		val groupedMetrics = groupMetrics(mapAsScalaMap(nameToMetric))
		for ((prefix, metricMap) <- groupedMetrics) {
			if (prefix.isEmpty) {
				// No prefix, process this metric by itself
				for ((name, metric) <- metricMap) {
					jsonForMetrics = jsonForMetrics :+ getJsonForMetric(metric, name)
				}
			}
			else {
				// This metric is part of a group, all of whom should be reported together
				jsonForMetrics = jsonForMetrics :+ getJsonForGroupedMetrics(groupedMetrics, prefix)
			}
		}
		jsonForMetrics
	}

  /** send latest metrics to Qasino */
  private def reportToQasino(nameToMetric: ListMap[String, Metric]): Unit = {
    // send a request for each table update
    val futures =
      getJson(nameToMetric) map { json =>
        log.info("JSON:" + json)
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

    val sanitizedRegistry = sanitizeRegistry(registry)

    reportToQasino(combineMetricsToMap(
      sanitizedRegistry.getGauges,
      sanitizedRegistry.getCounters,
      sanitizedRegistry.getHistograms,
      sanitizedRegistry.getMeters,
      sanitizedRegistry.getTimers
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
