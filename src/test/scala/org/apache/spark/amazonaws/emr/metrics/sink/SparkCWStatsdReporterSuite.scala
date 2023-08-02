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

import org.apache.spark.SparkFunSuite
import org.scalatest.PrivateMethodTester
import java.util.concurrent.TimeUnit._
import com.codahale.metrics._
import org.apache.spark.amazonaws.emr.metrics.sink.SparkCWStatsdReporter._

class SparkCWStatsdReporterSuite extends SparkFunSuite with PrivateMethodTester {
  val registry = new MetricRegistry
  val host = "127.0.0.1"
  val port = 8125
  val prefix = ""
  val jobflowId = "j-E0X1A2M3P4L5E"
  val instanceId = "i-0e1x2a3m4p5l6e7id"
  val privateIp = "ip-10-0-0-10"
  val sparkCWStatsdReporter = new SparkCWStatsdReporter(registry, host, port, prefix, jobflowId, instanceId, privateIp)

  test("makeTags method in SparkCWStatsdReporter") {
    val metricName = "application_0000000000000_0001.instance.namespace.SparkCWStatsdSinkTestMetricName"
    val metricSuffix = "count"
    val expectedShortName = "SparkCWStatsdSinkTestMetricName.count"

    val expectedTagString = "#ApplicationID:application_0000000000000_0001,MetricInstance:instance," +
      "Namespace:namespace,jobflowId:" + jobflowId + ",instanceID:" + instanceId + "," +
      "privateIp:" + privateIp + ",FullMetricName:" +
      "application_0000000000000_0001.instance.namespace.SparkCWStatsdSinkTestMetricName"
    val makeTagsMethod = PrivateMethod[(String, String)]('makeTags)

    val (resultTagString, resultShortName) = sparkCWStatsdReporter invokePrivate makeTagsMethod(metricName, metricSuffix)
    logInfo(s"Received makeTags result: '$resultShortName', '$resultTagString'")
    assert(resultShortName == expectedShortName && resultTagString == expectedTagString,
      "makeTags result received should match expected results")
  }
}

