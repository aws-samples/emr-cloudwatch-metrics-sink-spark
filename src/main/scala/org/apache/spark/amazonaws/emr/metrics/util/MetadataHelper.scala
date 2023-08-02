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
import java.io.FileNotFoundException
import java.io.IOException
import org.apache.spark.internal.Logging
import scala.io.Source

object MetadataHelper extends Logging {

  def getClusterID(): String = {
    val jobFlowStr = readFile(Constants.jobFlowFile)
    jobFlowStr.parseJson.asJsObject.getFields("jobFlowId")(0).toString().replace("\"", "")
  }

  def getInstanceID(): String ={
    val instanceIdStr = readFile(Constants.instanceIdFile)
    instanceIdStr.replace("\n", "")
  }

  def getPrivateIp(): String ={
    val privateIpStr = readFile(Constants.privateIpFile)
    privateIpStr.replace("\n", "")
  }

  def readFile(filePath: String): String ={
    val emptyJson = "[{}]"
    try {
      val bufferedSource = Source.fromFile(filePath)
      val returnString = bufferedSource.mkString
      bufferedSource.close
      if ("".equals(returnString)){
        logError("Empty value returned from file: " + filePath)
      }
      return returnString
    }
    catch {
      case e: FileNotFoundException => {
        logError(e.getMessage, e)
        return emptyJson
      }
      case e: IOException => {
        logError(e.getMessage, e)
        return emptyJson
      }
    }
  }
}