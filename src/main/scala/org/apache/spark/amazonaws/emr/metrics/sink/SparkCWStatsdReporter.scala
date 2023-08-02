/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.apache.spark.amazonaws.emr.metrics.sink

import java.io.IOException
import java.net.{DatagramPacket, DatagramSocket, InetSocketAddress}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.SortedMap
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import com.codahale.metrics._
import org.apache.hadoop.net.NetUtils
import org.apache.spark.amazonaws.emr.metrics.util.CustomMetricFilter
import org.apache.spark.internal.Logging

import java.time._

/**
 * @see <a href="https://github.com/etsy/statsd/blob/master/docs/metric_types.md">
 *        StatsD metric types</a>
 */
object StatsdMetricType {
  val COUNTER = "c"
  val GAUGE = "g"
  val TIMER = "ms"
  val Set = "s"
}

//fix indentation
class SparkCWStatsdReporter(
                          registry: MetricRegistry,
                          host: String = "127.0.0.1",
                          port: Int = 8125,
                          prefix: String = "",
                          jobflowId: String = "",
                          instanceId: String = "",
                          privateIp: String = "",
                          sparkAttemptSuffix: String = "",
                          filter: MetricFilter = MetricFilter.ALL,
                          rateUnit: TimeUnit = TimeUnit.SECONDS,
                          durationUnit: TimeUnit = TimeUnit.MILLISECONDS)
  extends ScheduledReporter(registry, "statsd-reporter", filter, rateUnit, durationUnit)
    with Logging {

  import StatsdMetricType._

  private val address = new InetSocketAddress(host, port)
  private val whitespace = "[\\s]+".r
  private var sparkAppId = ""
  private var checkpointEpoch = 0L

  override def report(
                    gauges: SortedMap[String, Gauge[_]],
                    counters: SortedMap[String, Counter],
                    histograms: SortedMap[String, Histogram],
                    meters: SortedMap[String, Meter],
                    timers: SortedMap[String, Timer]): Unit =
    Try(new DatagramSocket) match {
      case Failure(ioe: IOException) => logWarning("StatsD datagram socket construction failed",
        NetUtils.wrapException(host, port, NetUtils.getHostname(), 0, ioe))
      case Failure(e) => logWarning("StatsD datagram socket construction failed", e)
      case Success(s) =>
        implicit val socket = s
        val localAddress = Try(socket.getLocalAddress).map(_.getHostAddress).getOrElse(null)
        val localPort = socket.getLocalPort
        Try {
          var curEpoch = getEpoch()
          var secondsSinceLastPush = curEpoch - checkpointEpoch
          logDebug("secondsSinceLastPush: " + secondsSinceLastPush.toString)
          if (secondsSinceLastPush == 0) {
            Thread.sleep(1000)
          }
            gauges.entrySet.asScala.foreach(e => reportGauge(e.getKey, e.getValue))
            counters.entrySet.asScala.foreach(e => reportCounter(e.getKey, e.getValue))
            histograms.entrySet.asScala.foreach(e => reportHistogram(e.getKey, e.getValue))
            meters.entrySet.asScala.foreach(e => reportMetered(e.getKey, e.getValue))
            timers.entrySet.asScala.foreach(e => reportTimer(e.getKey, e.getValue))
            checkpointEpoch = curEpoch
        } recover {
          case ioe: IOException =>
            logError(s"Unable to send packets to StatsD", NetUtils.wrapException(
              address.getHostString, address.getPort, localAddress, localPort, ioe))
          case e: Throwable => logError(s"Unable to send packets to StatsD at '$host:$port'", e)
        }
        Try(socket.close()) recover {
          case ioe: IOException =>
            logError("Error when close socket to StatsD", NetUtils.wrapException(
              address.getHostString, address.getPort, localAddress, localPort, ioe))
          case e: Throwable => logError("Error when close socket to StatsD", e)
        }
    }

  private def reportGauge(name: String, gauge: Gauge[_])(implicit socket: DatagramSocket): Unit = {
    formatAny(gauge.getValue).foreach(v => send(fullName(name), v, GAUGE, (makeTags(name, ""))))
  }

  private def reportCounter(name: String, counter: Counter)(implicit socket: DatagramSocket): Unit = {
    send(fullName(name), format(counter.getCount), COUNTER, (makeTags(name, "")))
  }

  private def reportHistogram(name: String, histogram: Histogram)
                             (implicit socket: DatagramSocket): Unit = {
    val snapshot = histogram.getSnapshot
    send(fullName(name, "count"), format(histogram.getCount), GAUGE, (makeTags(name, "count")))
    send(fullName(name, "max"), format(snapshot.getMax), TIMER, (makeTags(name, "max")))
    send(fullName(name, "mean"), format(snapshot.getMean), TIMER, (makeTags(name, "mean")))
    send(fullName(name, "min"), format(snapshot.getMin), TIMER, (makeTags(name, "min")))
    send(fullName(name, "stddev"), format(snapshot.getStdDev), TIMER, (makeTags(name, "stddev")))
    send(fullName(name, "p50"), format(snapshot.getMedian), TIMER, (makeTags(name, "p50")))
    send(fullName(name, "p75"), format(snapshot.get75thPercentile), TIMER, (makeTags(name, "p75")))
    send(fullName(name, "p95"), format(snapshot.get95thPercentile), TIMER, (makeTags(name, "p95")))
    send(fullName(name, "p98"), format(snapshot.get98thPercentile), TIMER, (makeTags(name, "p98")))
    send(fullName(name, "p99"), format(snapshot.get99thPercentile), TIMER, (makeTags(name, "p99")))
    send(fullName(name, "p999"), format(snapshot.get999thPercentile), TIMER, (makeTags(name, "p999")))
  }

  private def reportMetered(name: String, meter: Metered)(implicit socket: DatagramSocket): Unit = {
    send(fullName(name, "count"), format(meter.getCount), GAUGE, (makeTags(name, "count")))
    send(fullName(name, "m1_rate"), format(convertRate(meter.getOneMinuteRate)), TIMER, (makeTags(name, "m1_rate")))
    send(fullName(name, "m5_rate"), format(convertRate(meter.getFiveMinuteRate)), TIMER, (makeTags(name, "m5_rate")))
    send(fullName(name, "m15_rate"), format(convertRate(meter.getFifteenMinuteRate)), TIMER, (makeTags(name, "m15_rate")))
    send(fullName(name, "mean_rate"), format(convertRate(meter.getMeanRate)), TIMER, (makeTags(name, "mean_rate")))
  }

  private def reportTimer(name: String, timer: Timer)(implicit socket: DatagramSocket): Unit = {
    val snapshot = timer.getSnapshot
    send(fullName(name, "max"), format(convertDuration(snapshot.getMax)), TIMER, (makeTags(name, "max")))
    send(fullName(name, "mean"), format(convertDuration(snapshot.getMean)), TIMER, (makeTags(name, "mean")))
    send(fullName(name, "min"), format(convertDuration(snapshot.getMin)), TIMER, (makeTags(name, "min")))
    send(fullName(name, "stddev"), format(convertDuration(snapshot.getStdDev)), TIMER, (makeTags(name, "stddev")))
    send(fullName(name, "p50"), format(convertDuration(snapshot.getMedian)), TIMER, (makeTags(name, "p50")))
    send(fullName(name, "p75"), format(convertDuration(snapshot.get75thPercentile)), TIMER, (makeTags(name, "p75")))
    send(fullName(name, "p95"), format(convertDuration(snapshot.get95thPercentile)), TIMER, (makeTags(name, "p95")))
    send(fullName(name, "p98"), format(convertDuration(snapshot.get98thPercentile)), TIMER, (makeTags(name, "p98")))
    send(fullName(name, "p99"), format(convertDuration(snapshot.get99thPercentile)), TIMER, (makeTags(name, "p99")))
    send(fullName(name, "p999"), format(convertDuration(snapshot.get999thPercentile)), TIMER, (makeTags(name, "p999")))

    reportMetered(name, timer)
  }

  private def send(name: String, value: String, metricType: String, tagAndName: (String, String))
                  (implicit socket: DatagramSocket): Unit = {
    if (CustomMetricFilter.filterMetrics(name)){
      val metricTags = tagAndName._1
      val shortName = tagAndName._2
      logDebug("Sending Metric: " + name + " with value: " + value.toString)
      val bytes = sanitize(s"$shortName:$value|$metricType|@1.0|$metricTags").getBytes(UTF_8)
      val packet = new DatagramPacket(bytes, bytes.length, address)
      socket.send(packet)
    }
  }

  private def getEpoch(): Long = {
    var curTime: LocalDateTime = LocalDateTime.now()
    var curZdt: ZonedDateTime = curTime.atZone(ZoneId.of("UTC"))
    var curTimeInMillis = curZdt.toInstant.toEpochMilli
    var curEpoch = curTimeInMillis / 1000L
    return curEpoch
  }


  private def makeTags(metricName: String, metricSuffix: String): (String, String) = {
    var nameSplit = metricName.split('.')
    var metricInstance = ""
    var nameSpace = ""
    var tagString = ""
    var shortName = ""
    var appId = ""
    if (nameSplit.length >= 3) {
      if (nameSplit(0).substring(0,11) == "application") {
        appId = nameSplit(0)
        sparkAppId = appId + sparkAttemptSuffix
        if (nameSplit(1).forall(Character.isDigit)) {
          metricInstance = "executorID_" + nameSplit(1)
        }
        else {
          metricInstance = nameSplit(1)
        }
        nameSpace = nameSplit(2)
        shortName = nameSplit.drop(3).mkString(".")
      }
      else {
        metricInstance = nameSplit(0)
        shortName = nameSplit.drop(1).mkString(".")
      }
      if ("".equals(appId)) {
        appId = sparkAppId
      }
      if (metricInstance == "applicationMaster") {
        shortName = nameSplit.drop(1).mkString(".")
        nameSpace = metricInstance
      }
    }
    if (metricSuffix != "") {
      shortName = shortName + "." + metricSuffix
    }
    tagString = "#ApplicationID:" + sparkAppId + ",MetricInstance:" + metricInstance + ",Namespace:" +
      nameSpace + ",jobflowId:" + jobflowId + ",instanceID:" + instanceId + ",privateIp:" + privateIp +
    ",FullMetricName:" + metricName
    return (tagString, shortName)
  }

  private def fullName(names: String*): String = {
    var numI = names.length
    var newNames = new Array[String](numI)
    for ( i <- 0 to (names.length - 1)) {
      newNames(i) = names(i).replaceAll("_", ".")
    }
    MetricRegistry.name(prefix, newNames : _*)
  }

  private def sanitize(s: String): String = {
    whitespace.replaceAllIn(s, "-")
  }

  private def format(v: Any): String = formatAny(v).getOrElse("")

  private def formatAny(v: Any): Option[String] =
    v match {
      case f: Float => Some("%2.2f".format(f))
      case d: Double => Some("%2.2f".format(d))
      case b: BigDecimal => Some("%2.2f".format(b))
      case n: Number => Some(v.toString)
      case _ => None
    }
}

