package mediamath.metrics

import com.codahale.metrics._
import com.fasterxml.jackson.databind.ObjectMapper
import dispatch._
import Defaults._
import java.util.concurrent.TimeUnit
import scala.collection
import scala.collection.JavaConversions._
import java.net.InetAddress
import collection._
import java.util.{SortedMap => JavaSortedMap}

/**
 * Created by dpowell on 1/26/14.
 */

object QasinoReporter {
	val registryNameSeparator = "_"
	val illegalCharRegex = new scala.util.matching.Regex("""[^A-Za-z0-9_]""")

	def sanitizeRegistryName(name: String): String = {
		// Remove any instances of the illegal characters from the name
		illegalCharRegex.replaceAllIn(name.toLowerCase, registryNameSeparator)
	}

	def sanitizeRegistry(registry: MetricRegistry): MetricRegistry = {
		// Return a new MetricRegistry with names sanitized for qasino
		val sanitizedRegistry = new MetricRegistry
		val metricMap = mapAsScalaMap(registry.getMetrics)
		for ((name, metric) <- metricMap) {
			sanitizedRegistry.register(sanitizeRegistryName(name), metric)
		}
		sanitizedRegistry
	}
}

class QasinoReporterBuilder (
		var registry: MetricRegistry = new MetricRegistry,
		var host: String = "127.0.0.1",
		var port: Int = 80,
		var secure: Boolean = false,
		var uri: String = "/request?op=add_table_data",
		var name: String = "QasinoReporter",
		var gaugeGroups: Set[String] = SortedSet.empty,
		var filter: MetricFilter = MetricFilter.ALL,
		var rateUnit: TimeUnit = TimeUnit.SECONDS,
		var durationUnit: TimeUnit = TimeUnit.MILLISECONDS) {

	private[this] def registryHasCollisions: Boolean = {
		// Check whether we have any name collisions after some sanitizing
		val namesSet = mutable.Set[String]()
		val registryNames = registry.getNames
		for (name <- asScalaSet(registryNames)) {
			val sanitizedName = QasinoReporter.sanitizeRegistryName(name)
			namesSet.add(sanitizedName)
		}
		namesSet.size < registryNames.size()
	}

	def withRegistry(registry: MetricRegistry): QasinoReporterBuilder = {
		this.registry = registry
		this
	}

	def withPort(port: Int): QasinoReporterBuilder = {
		this.port = port
		this
	}

	def withDest(host: String): QasinoReporterBuilder = {
		this.host = host
		this
	}

	def secure(secure: Boolean = true): QasinoReporterBuilder = {
		this.secure = secure
		this
	}

	def withUri(uri: String): QasinoReporterBuilder = {
		this.uri = uri
		this
	}

	def withName(name: String): QasinoReporterBuilder = {
		this.name = name
		this
	}

	def withGaugeGroups(gaugeGroups: Set[String]): QasinoReporterBuilder = {
		this.gaugeGroups = gaugeGroups
		this
	}

	def withFilter(filter: MetricFilter): QasinoReporterBuilder = {
		this.filter = filter
		this
	}

	def build(): QasinoReporter = {
		if (registryHasCollisions) {
			throw new IllegalArgumentException(
				"Found a collision within registry names after sanitation"
			)
		}
		registry = QasinoReporter.sanitizeRegistry(registry)
		new QasinoReporter(this)
	}
}

class QasinoReporter(builder: QasinoReporterBuilder) extends
		ScheduledReporter(builder.registry, builder.name, builder.filter, builder.rateUnit, builder.durationUnit) {
	val registry: MetricRegistry = builder.registry
	val host: String = builder.host
	val port: Int = builder.port
	val secure: Boolean = builder.secure
	val uri: String = builder.uri
	val name: String = builder.name
	val gaugeGroups: Set[String] = builder.gaugeGroups
	val filter: MetricFilter = builder.filter
	val rateUnit: TimeUnit = builder.rateUnit
	val durationUnit: TimeUnit = builder.durationUnit

	// Set up Dispatch HTTP client
	private val dispatchHost = if (secure) dispatch.host(host, port).secure else dispatch.host(host, port)
	private val dispatchRequest = (dispatchHost / uri).POST

	val inetAddr = InetAddress.getLocalHost

	// JSON mapper singleton
	private val mapper = new ObjectMapper()

	object QasinoRequestIdentifier extends scala.Enumeration {
		// Enumeration for all the JSON keys for qasino for safety
		type QasinoRequestIdentifier = Value
		val op, identity, table, tablename, column_names, column_types, rows = Value
	}
	import QasinoRequestIdentifier._

	// Default map for JSON
	private val defaultDataJson = mutable.Map[String, Any](
		op.toString -> "add_table_data",
		tablename.toString -> Unit,
		column_names.toString-> Unit,
		column_types.toString -> Unit,
		identity.toString -> inetAddr.toString // This provides the hostname and IP joined by a forward slash
	)

	// Shorthand for a two dimensional map of any type
	type TwoDMap[K1, K2, Val] = Map[K1, Map[K2, Val]]

	def getColumnNames(metric: Metric, prefixWithSeparator: String = ""): SortedSet[String] = metric match {
		// Get the qasino column names for any metric type
		case gauge: Gauge[_] =>
			SortedSet("value") map {prefixWithSeparator + _}
		case _: Counter =>
			SortedSet("count") map {prefixWithSeparator + _}
		case _: Histogram =>
			SortedSet("min", "max", "mean", "median") map {prefixWithSeparator + _}
		case _: Meter =>
			SortedSet("one_minute_rate", "five_minute_rate", "fifteen_minute_rate", "mean_rate") map {prefixWithSeparator + _}
		case _: Timer =>
			SortedSet("one_minute_rate", "five_minute_rate", "fifteen_minute_rate", "mean_rate") map {prefixWithSeparator + _}
		case _ => SortedSet.empty[String]
	}

	def getGroupedColumnNames(groupedMetrics: TwoDMap[String, String, Metric], prefix: String):
			SortedSet[String] = {
		var groupColumnNames = SortedSet.empty[String]
		val metricMap = groupedMetrics.getOrElse(prefix, Map.empty[String, Metric])
		for ((suffix, metric) <- metricMap) {
			val thisMetricColumnNames = getColumnNames(metric, suffix + "_")
			groupColumnNames = groupColumnNames ++ thisMetricColumnNames
		}
		groupColumnNames
	}

	def getColumnTypes(metric: Metric, prefix: String = ""): Seq[String] = metric match {
		// Get the qasino column types for any metric type
		case _: Gauge[_] => Seq("string")
		case _: Counter => Seq("int")
		case _: Histogram => Seq("int", "int", "int", "int")
		case _: Meter => Seq("int", "int", "int", "int")
		case _: Timer => Seq("int", "int", "int", "int")
		case _ => Seq.empty[String]
	}

	def getGroupedColumnTypes(groupedMetrics: TwoDMap[String, String, Metric], prefix: String):
			Seq[String] = {
		var groupColumnTypes = Seq.empty[String]
		val metricMap = groupedMetrics.getOrElse(prefix, Map.empty[String, Metric])
		for ((_, metric) <- metricMap) {
			val thisMetricColumnTypes = getColumnTypes(metric)
			groupColumnTypes = groupColumnTypes ++ thisMetricColumnTypes
		}
		groupColumnTypes
	}

	def getColumnValues(metric: Metric) = metric match {
		// Get the qasino column values for any metric type
		case gauge: Gauge[_] =>
			Array(gauge.getValue.toString)
		case counter: Counter => Array(counter.getCount)
		case histogram: Histogram => {
			val snap = histogram.getSnapshot
			Array(snap.getMin, snap.getMax, snap.getMean, snap.getMedian)
		}
		case meter: Meter =>
			Array(meter.getOneMinuteRate, meter.getFiveMinuteRate, meter.getFifteenMinuteRate, meter.getMeanRate)
		case timer: Timer =>
			Array(timer.getOneMinuteRate, timer.getFiveMinuteRate, timer.getFifteenMinuteRate, timer.getMeanRate)
	}

	def getGroupedColumnValues(groupedMetrics: TwoDMap[String, String, Metric], prefix: String):
	Array[Any] = {
		var groupColumnValues = Array.empty[Any]
		val metricMap = groupedMetrics.getOrElse(prefix, Map.empty[String, Metric])
		for ((_, metric) <- metricMap) {
			val thisMetricColumnValues = getColumnValues(metric)
			groupColumnValues = groupColumnValues ++ thisMetricColumnValues
		}
		groupColumnValues
	}

	def getJsonForMetric(metric: Metric): String = {
		// Get the qasino json data for any metric type
		val postDataMap = defaultDataJson
		postDataMap(tablename.toString) = metric.toString
		postDataMap(column_names.toString) = setAsJavaSet(getColumnNames(metric))
		postDataMap(column_types.toString) = seqAsJavaList(getColumnTypes(metric))
		postDataMap(rows.toString) = seqAsJavaList(getColumnValues(metric))
		mapper.writeValueAsString(mapAsJavaMap(postDataMap))
	}

	def getGroupedJson(groupedMetrics: TwoDMap[String, String, Metric], prefix: String): String = {
		// Get the qasino json data for any grouped metric type
		val postDataMap = defaultDataJson
		postDataMap(tablename.toString) = prefix
		postDataMap(column_names.toString) = setAsJavaSet(getGroupedColumnNames(groupedMetrics, prefix))
		postDataMap(column_types.toString) = seqAsJavaList(getGroupedColumnTypes(groupedMetrics, prefix))
		postDataMap(rows.toString) = seqAsJavaList(getGroupedColumnValues(groupedMetrics, prefix))
		mapper.writeValueAsString(mapAsJavaMap(postDataMap))
	}

	def groupMetrics(metrics: Map[String, Metric]): TwoDMap[String, String, Metric] = {
		var groupedMetrics: TwoDMap[String, String, Metric] = Map.empty
		val emptryString = ""
		for ((name, metric) <- metrics) {
			// Match groups going by longest group name to shortest
			val thisGaugeGroup: Option[String] =
				gaugeGroups.toSeq.sortBy(_.length).reverse.find(s => name.startsWith(s + "_"))
			val suffix: String = if (thisGaugeGroup.isDefined) {
				// Add one to the length for the separator
				name.drop(thisGaugeGroup.get.length + 1)
			}
			else name
			var subgroupedMetrics: Map[String, Metric] = groupedMetrics
				.getOrElse(thisGaugeGroup.getOrElse(emptryString), Map.empty)
			subgroupedMetrics = subgroupedMetrics + (suffix -> metric)
			groupedMetrics = groupedMetrics + (thisGaugeGroup.getOrElse(emptryString) -> subgroupedMetrics)
		}
		groupedMetrics
	}

	override def report (
			gauges: JavaSortedMap[String, Gauge[_]],
			counters: JavaSortedMap[String, Counter],
			histograms: JavaSortedMap[String, Histogram],
			meters: JavaSortedMap[String, Meter],
			timers: JavaSortedMap[String, Timer]) {

		for(nameToMetric <- Seq(gauges, counters, histograms, meters, timers)) {
			// Generate the JSON data and send the POST request to the qasino daemon
			val groupedMetrics = groupMetrics(mapAsScalaMap(nameToMetric))
			for ((prefix, metricMap) <- groupedMetrics) {
				if (prefix.isEmpty) {
					// No prefix, process this metric by itself
					for ((name, metric) <- metricMap) {
						val postDataJson = getJsonForMetric(metric)
						val postWithParams = dispatchRequest << postDataJson
						dispatch.Http(postWithParams OK as.String)
					}
				}
				else {
					// This metric is part of a group, all of whom should be reported together
					val postDataJson = getGroupedJson(groupedMetrics, prefix)
					val postWithParams = dispatchRequest << postDataJson
					dispatch.Http(postWithParams OK as.String)
				}
			}
		}
	}
}

/* Some test code for the REPL follows
import com.codahale.metrics.{Counter, MetricRegistry}
var metrics = new MetricRegistry
var counter1 = new Counter
var counter2 = new Counter
metrics.register(MetricRegistry.name("testing.123"), counter1)
counter2.inc(100)
metrics.register(MetricRegistry.name("testing-345"), counter2)
var reporter = new mediamath.metrics.QasinoReporterBuilder().withDest("www.imadethatcow.com").withName("testing123").withRegistry(metrics).withGaugeGroups(Set("testing")).build()
reporter.report()
 */ // TODO: remove this test code
