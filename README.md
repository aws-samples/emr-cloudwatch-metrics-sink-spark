# EMR Custom Spark CloudWatch Sink


This is a metrics sink based on the standard [Spark StatsdSink](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/metrics/sink/StatsdSink.scala) 
class, with modifications to be compatible with the standard AWS [CloudWatch Agent](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Agent-custom-metrics-statsd.html
).

Required metrics should be defined in the `Metricfilter.json` file, based on the Spark monitoring [documentation](https://spark.apache.org/docs/3.0.0/monitoring.html).

## Building

There are two profiles defined in the maven pom file: "emr-5.x.x" and "emr-6.x.x". To build, specify the correct profile.

Example for building the jar to be compatible on EMR 6.x releases:
```
mvn clean -P emr-6.x.x
mvn package -P emr-6.x.x
```

## Setup

**Note: Select the correct JAR depending on the EMR release used (./emr-5.x vs ./emr-6.x)**
Pre-built jars are available here:
* EMR-6.x.x: [spark-emr-cloudwatch-sink-0.0.1.jar](s3://aws-blogs-artifacts-public/artifacts/BDB-3297/emr-6.x.x/spark-emr-cloudwatch-sink-0.0.1.jar)
* EMR-5.x.x: [spark-emr-cloudwatch-sink-0.0.1.jar](s3://aws-blogs-artifacts-public/artifacts/BDB-3297/emr-5.x.x/spark-emr-cloudwatch-sink-0.0.1.jar)


1. Make use of the Spark monitoring [documentation](https://spark.apache.org/docs/3.0.0/monitoring.html) to define the metrics you require in the `Metricfilter.json` file
2. Upload the `spark-emr-cloudwatch-sink-0.0.1.jar`, `Metricfilter.json` and `installer.sh` to an S3 bucket accessible from your EMR cluster
3. While launching a cluster, specify the following configuration and Bootstrap Action:
```
aws emr create-cluster ...
--bootstrap-actions '[
    {
        "Name": "Install EMRCustomSparkCloudWatchSink",
        "Path": "s3://<bucketname>/<path>/<to>/installer.sh",
        "Args":
        [
            "s3://<bucketname>/<path>/<to>/spark-emr-cloudwatch-sink-0.0.1.jar",
            "s3://<bucketname>/<path>/<to>/Metricfilter.json",
            "CustomCloudwatchNamespace"
        ]
    }
]'
...
--configurations '[
  {
    "Classification": "spark-metrics",
    "Properties": {
      "*.sink.statsd.class": "org.apache.spark.amazonaws.emr.metrics.sink.SparkCWStatsdSink",
      "*.sink.statsd.prefix": "spark",
      "*.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
      "driver.sink.statsd.tags": "driver",
      "executor.sink.statsd.tags": "executor"
    }
  },
  {
    "Classification": "spark-defaults",
    "Properties": {
      "spark.metrics.appStatusSource.enabled": "true",
      "spark.metrics.staticSources.enabled": "true"
    }
  }
]'
...
```
4. Run a spark job
```
spark-submit --class org.apache.spark.examples.SparkPi /usr/lib/spark/examples/jars/spark-examples.jar 10000
```

## Viewing Metrics
Metrics should be available in the Cloudwatch console a few minutes after running the spark job. See `ExampleMetrics.png`. 

## Troubleshooting

This has been tested on EMR releases 5.13.1 through 6.9.0

1. Check that the cloudwatch agent launched successfully: `/var/log/amazon/amazon-cloudwatch-agent/amazon-cloudwatch-agent.log`
2. Verify that the configuration file is present and valid on cluster nodes: `/etc/amazon/EMRCustomSparkCloudWatchSink/Metricfilter.json`
3. Verify that the JAR is present and valid on cluster nodes: `/usr/lib/spark/jars/spark-emr-cloudwatch-sink-0.0.1.jar`
4. Check spark application logs for errors: 
```
[hadoop@ip-10-0-0-60 ~]$ yarn logs -applicationId application_1676059760810_0002 | grep "SparkCW"
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/lib/hadoop/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/share/aws/emr/emrfs/lib/slf4j-log4j12-1.7.12.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
2023-02-10 20:22:31,349 INFO client.RMProxy: Connecting to ResourceManager at ip-10-0-0-60.ec2.internal/10.0.0.60:8032
2023-02-10 20:22:31,543 INFO client.AHSProxy: Connecting to Application History server at ip-10-0-0-60.ec2.internal/10.0.0.60:10200
23/02/10 20:14:24 INFO SparkCWStatsdSink: StatsdSink started with prefix: 'spark'
23/02/10 20:14:34 INFO SparkCWStatsdReporter: Sending Metric: spark.application.1676059760810.0002.2.ExecutorMetrics.MajorGCTime
23/02/10 20:14:34 INFO SparkCWStatsdReporter: Sending Metric: spark.application.1676059760810.0002.2.ExecutorMetrics.MinorGCTime
23/02/10 20:14:34 INFO SparkCWStatsdReporter: Sending Metric: spark.application.1676059760810.0002.2.JVMCPU.jvmCpuTime
23/02/10 20:14:34 INFO SparkCWStatsdReporter: Sending Metric: spark.application.1676059760810.0002.2.executor.diskBytesSpilled
23/02/10 20:14:34 INFO SparkCWStatsdReporter: Sending Metric: spark.application.1676059760810.0002.2.executor.recordsRead
23/02/10 20:14:34 INFO SparkCWStatsdReporter: Sending Metric: spark.application.1676059760810.0002.2.executor.recordsWritten
```


## Changelog
* Added blog post draft with walkthrough and cloudformation example
* Fixed an issue where applicationMaster metrics were not getting tagged properly
* Fixed a bug where datapoints may get duplicated as executors/driver jvms are terminated
* Added test classes
* Fixed a bug where file handles were not closed
* Fixed a bug where timer and histogram metrics did not have suffixes
* Fixed a bug with EMR 6.6 related to https://issues.apache.org/jira/browse/SPARK-37078
