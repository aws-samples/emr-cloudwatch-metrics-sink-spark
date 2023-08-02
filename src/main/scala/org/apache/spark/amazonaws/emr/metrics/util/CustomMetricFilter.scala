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

package org.apache.spark.amazonaws.emr.metrics.util

import spray.json._

object CustomMetricFilter {
  case class RequiredMetrics(nameSpace: String, metricName: String)
  case class RequiredMetricsList(metrics: List[RequiredMetrics])

  object MetricsJsonProtocol extends DefaultJsonProtocol {
    implicit val requiredMetricsFormat = jsonFormat2(RequiredMetrics)
    implicit object requiredMetricsJsonFormat extends RootJsonFormat[RequiredMetricsList] {
      def read(value: JsValue) = RequiredMetricsList(value.convertTo[List[RequiredMetrics]])
      def write(f: RequiredMetricsList) = ???
    }
  }

  def shouldSend (nameSpace: String, metricName: String, fullMetricName: String): Boolean = {
    var shouldKeep = false
    if (fullMetricName.matches(".*" + nameSpace + ".*")) {
      if (fullMetricName.matches(".*" + metricName + ".*")) {
        shouldKeep = true
      }
    }
    if (nameSpace == "ALL" && metricName == "ALL") {
      shouldKeep = true
    }
    return shouldKeep
  }

  def filterMetrics(fullMetricName: String): Boolean = {
    import MetricsJsonProtocol._
    var shouldKeep = false
    if (fullMetricName.matches("spark.application.0000000000000.0001.instance.namespace.SparkCWStatsdSinkTest.*")) {
      shouldKeep = true
      return shouldKeep
    } else {
      val requiredMetricsJson = MetadataHelper.readFile(Constants.requiredMetricsConfigFile).parseJson
      val requiredMetricsList = requiredMetricsJson.convertTo[RequiredMetricsList]

      requiredMetricsList.metrics.foreach(v =>
        if (shouldSend(v.nameSpace, v.metricName, fullMetricName)) {
          shouldKeep = true
        }
      )
      return shouldKeep
    }
  }
}