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

import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit
import com.codahale.metrics.MetricRegistry
import org.apache.spark.SecurityManager
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.sink.Sink
import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.amazonaws.emr.metrics.util.MetadataHelper
import org.apache.spark.internal.config

object SparkCWStatsdSink {
  val STATSD_KEY_HOST = "host"
  val STATSD_KEY_PORT = "port"
  val STATSD_KEY_PERIOD = "period"
  val STATSD_KEY_UNIT = "unit"
  val STATSD_KEY_PREFIX = "prefix"

  val STATSD_DEFAULT_HOST = "127.0.0.1"
  val STATSD_DEFAULT_PORT = "8125"
  val STATSD_DEFAULT_PERIOD = "30"
  val STATSD_DEFAULT_UNIT = "SECONDS"
  val STATSD_DEFAULT_PREFIX = ""
}

class SparkCWStatsdSink(
                         val property: Properties,
                         val registry: MetricRegistry)
  extends Sink with Logging {

  //Legacy Constructor required by MetricsSystem::registerSinks() for spark < 3.2
  def this(property: Properties, registry: MetricRegistry, securityMgr: SecurityManager) = {
    this(
      property,
      registry
    )
  }
  import SparkCWStatsdSink._

  val host = property.getProperty(STATSD_KEY_HOST, STATSD_DEFAULT_HOST)
  val port = property.getProperty(STATSD_KEY_PORT, STATSD_DEFAULT_PORT).toInt
  val jobflowId = MetadataHelper.getClusterID()
  val instanceId = MetadataHelper.getInstanceID()
  val privateIp = MetadataHelper.getPrivateIp()

  private[this] val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
  private[this] val MINIMAL_POLL_PERIOD = 1
  private lazy val sparkConfig = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf(true))

  // Don't use sparkConf.getOption("spark.metrics.namespace") as the underlying string won't be substituted.
  def metricsNamespace: Option[String] = sparkConfig.get(config.METRICS_NAMESPACE)
  def sparkAppId: Option[String] = sparkConfig.getOption("spark.app.id")
  def sparkAppName: Option[String] = sparkConfig.getOption("spark.app.name")
  def executorId: Option[String] = sparkConfig.getOption("spark.executor.id")
  // only available in cluster mode
  def attemptId: Option[String] = sparkConfig.getOption("spark.app.attempt.id")
  var sparkAttemptSuffix = ""
  if (attemptId.isDefined) {
    sparkAttemptSuffix = "_" + attemptId.get
  }
  else {
    sparkAttemptSuffix = "_1"
  }

  val pollPeriod = property.getProperty(STATSD_KEY_PERIOD, STATSD_DEFAULT_PERIOD).toInt
  val pollUnit =
    TimeUnit.valueOf(
      property.getProperty(STATSD_KEY_UNIT, STATSD_DEFAULT_UNIT).toUpperCase(Locale.ROOT))

  val prefix = property.getProperty(STATSD_KEY_PREFIX, STATSD_DEFAULT_PREFIX)

  checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val reporter = new SparkCWStatsdReporter(registry, host, port, prefix, jobflowId, instanceId, privateIp, sparkAttemptSuffix)

  override def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
    logInfo(s"SparkCWStatsdSink started with prefix: '$prefix'")
  }

  override def stop(): Unit = {
    reporter.stop()
    logInfo("SparkCWStatsdSink stopped.")
  }

  override def report(): Unit = reporter.report()

  def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int): Unit = {
    val period = MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit)
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }

}