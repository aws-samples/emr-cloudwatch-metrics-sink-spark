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

import java.net.{DatagramPacket, DatagramSocket}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Properties
import java.util.concurrent.TimeUnit._
import com.codahale.metrics._
import org.apache.spark.SparkFunSuite
import SparkCWStatsdSink._

class SparkCWStatsdSinkSuite extends SparkFunSuite {
  private val defaultProps = Map(
    STATSD_KEY_PREFIX -> "spark",
    STATSD_KEY_PERIOD -> "1",
    STATSD_KEY_UNIT -> "seconds",
    STATSD_KEY_HOST -> "127.0.0.1",
    STATSD_KEY_PORT -> "port"
  )
  // The maximum size of a single datagram packet payload. Payloads
  // larger than this will be truncated.
  private val maxPayloadSize = 256 // bytes

  // The receive buffer must be large enough to hold all inflight
  // packets. This includes any kernel and protocol overhead.
  // This value was determined experimentally and should be
  // increased if timeouts are seen.
  private val socketMinRecvBufferSize = 16384 // bytes
  private val socketTimeout = 30000           // milliseconds

  private def withSocketAndSink(testCode: (DatagramSocket, SparkCWStatsdSink) => Any): Unit = {
    val socket = new DatagramSocket

    // Leave the receive buffer size untouched unless it is too
    // small. If the receive buffer is too small packets will be
    // silently dropped and receive operations will timeout.
    if (socket.getReceiveBufferSize() < socketMinRecvBufferSize) {
      socket.setReceiveBufferSize(socketMinRecvBufferSize)
    }

    socket.setSoTimeout(socketTimeout)
    val props = new Properties
    defaultProps.foreach(e => props.put(e._1, e._2))
    props.put(STATSD_KEY_PORT, socket.getLocalPort.toString)
    val registry = new MetricRegistry
    val sink = new SparkCWStatsdSink(props, registry)
    try {
      testCode(socket, sink)
    } finally {
      socket.close()
    }
  }

  test("metrics SparkCWStatsD sink with Counter") {
    withSocketAndSink { (socket, sink) =>
      sink.start()
      val counter = new Counter
      counter.inc(12)
      sink.registry.register("application_0000000000000_0001.instance.namespace.SparkCWStatsdSinkTestCounter", counter)
      sink.report()

      val p = new DatagramPacket(new Array[Byte](maxPayloadSize), maxPayloadSize)
      socket.receive(p)

      val result = new String(p.getData, 0, p.getLength, UTF_8)
      val subResult = result.substring(0,33)
      assert(subResult === "SparkCWStatsdSinkTestCounter:12|c", "Counter metric received should match data sent")
    }
  }

  test("metrics SparkCWStatsD sink with Gauge") {
    withSocketAndSink { (socket, sink) =>
      val gauge = new Gauge[Double] {
        override def getValue: Double = 1.23
      }

      sink.registry.register("application_0000000000000_0001.instance.namespace.SparkCWStatsdSinkTestGauge", gauge)
      sink.report()

      val p = new DatagramPacket(new Array[Byte](maxPayloadSize), maxPayloadSize)
      socket.receive(p)

      val result = new String(p.getData, 0, p.getLength, UTF_8)
      val subResult = result.substring(0,33)
      assert(subResult === "SparkCWStatsdSinkTestGauge:1.23|g", "Gauge metric received should match data sent")
    }
  }

  test("metrics SparkCWStatsD sink with Histogram") {
    withSocketAndSink { (socket, sink) =>
      val p = new DatagramPacket(new Array[Byte](maxPayloadSize), maxPayloadSize)
      val histogram = new Histogram(new UniformReservoir)
      histogram.update(10)
      histogram.update(20)
      histogram.update(30)
      sink.registry.register("application_0000000000000_0001.instance.namespace.SparkCWStatsdSinkTestHistogram", histogram)
      sink.report()

      val expectedResults = Set(
        "SparkCWStatsdSinkTestHistogram.count:3|g",
        "SparkCWStatsdSinkTestHistogram.max:30|ms",
        "SparkCWStatsdSinkTestHistogram.mean:20.00|ms",
        "SparkCWStatsdSinkTestHistogram.min:10|ms",
        "SparkCWStatsdSinkTestHistogram.stddev:10.00|ms",
        "SparkCWStatsdSinkTestHistogram.p50:20.00|ms",
        "SparkCWStatsdSinkTestHistogram.p75:30.00|ms",
        "SparkCWStatsdSinkTestHistogram.p95:30.00|ms",
        "SparkCWStatsdSinkTestHistogram.p98:30.00|ms",
        "SparkCWStatsdSinkTestHistogram.p99:30.00|ms",
        "SparkCWStatsdSinkTestHistogram.p999:30.00|ms"
      )
      (1 to expectedResults.size).foreach { i =>
        socket.receive(p)
        val result = new String(p.getData, 0, p.getLength, UTF_8)
        val subResultLst = result.split('|')
        val subResult = subResultLst(0) + "|" + subResultLst(1)
        logInfo(s"Received histogram result $i: '$result'")
        assert(expectedResults.contains(subResult),
          "Histogram metric received should match data sent")
      }
    }
  }

  test("metrics SparkCWStatsD sink with Timer") {
    withSocketAndSink { (socket, sink) =>
      val p = new DatagramPacket(new Array[Byte](maxPayloadSize), maxPayloadSize)
      val timer = new Timer()
      timer.update(1, SECONDS)
      timer.update(2, SECONDS)
      timer.update(3, SECONDS)
      sink.registry.register("application_0000000000000_0001.instance.namespace.SparkCWStatsdSinkTestTimer", timer)
      sink.report()

      val expectedResults = Set(
        "SparkCWStatsdSinkTestTimer.max:3000.00|ms",
        "SparkCWStatsdSinkTestTimer.mean:2000.00|ms",
        "SparkCWStatsdSinkTestTimer.min:1000.00|ms",
        "SparkCWStatsdSinkTestTimer.stddev:1000.00|ms",
        "SparkCWStatsdSinkTestTimer.p50:2000.00|ms",
        "SparkCWStatsdSinkTestTimer.p75:3000.00|ms",
        "SparkCWStatsdSinkTestTimer.p95:3000.00|ms",
        "SparkCWStatsdSinkTestTimer.p98:3000.00|ms",
        "SparkCWStatsdSinkTestTimer.p99:3000.00|ms",
        "SparkCWStatsdSinkTestTimer.p999:3000.00|ms",
        "SparkCWStatsdSinkTestTimer.count:3|g",
        "SparkCWStatsdSinkTestTimer.m1_rate:0.00|ms",
        "SparkCWStatsdSinkTestTimer.m5_rate:0.00|ms",
        "SparkCWStatsdSinkTestTimer.m15_rate:0.00|ms"
      )
      // mean rate varies on each test run
      val oneMoreResult = """SparkCWStatsdSinkTestTimer.mean_rate:\d+\.\d\d\|ms"""

      (1 to (expectedResults.size + 1)).foreach { i =>
        socket.receive(p)
        val result = new String(p.getData, 0, p.getLength, UTF_8)
        val subResultLst = result.split('|')
        val subResult = subResultLst(0) + "|" + subResultLst(1)
        logInfo(s"Received timer result $i: '$result'")
        assert(expectedResults.contains(subResult) || subResult.matches(oneMoreResult),
          "Timer metric received should match data sent")
      }
    }
  }
}

