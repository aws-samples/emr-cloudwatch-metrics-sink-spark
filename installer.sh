#!/bin/bash
# usage
# Bootstrap action
# installer.sh <s3://bucketname/location/of/jarfile> <s3://bucketname/location/of/Metricfilter.json> <CloudWatchNamespace>

#Download Jars and config
jarLocation=$1
configLocation=$2
CWNamespace=$3

mkdir /tmp/EMRCustomSparkCloudWatchSink

aws s3 cp $jarLocation /tmp/EMRCustomSparkCloudWatchSink/
sudo mkdir -p /usr/lib/spark/jars/
sudo mv /tmp/EMRCustomSparkCloudWatchSink/* /usr/lib/spark/jars/

aws s3 cp $configLocation /tmp/EMRCustomSparkCloudWatchSink/
sudo mkdir -p /etc/amazon/EMRCustomSparkCloudWatchSink/
sudo mv /tmp/EMRCustomSparkCloudWatchSink/Metricfilter.json /etc/amazon/EMRCustomSparkCloudWatchSink/
sudo chmod 644 /etc/amazon/EMRCustomSparkCloudWatchSink/Metricfilter.json

# Install CloudWatch Agent
ARCH=$(uname -m)
URLARCH="amd64"
if ["$ARCH" == "aarch64"]; then
  URLARCH="arm64"
fi

cd /tmp/EMRCustomSparkCloudWatchSink/
wget https://s3.amazonaws.com/amazoncloudwatch-agent/linux/$URLARCH/latest/AmazonCloudWatchAgent.zip -O AmazonCloudWatchAgent.zip
unzip -o AmazonCloudWatchAgent.zip
sudo ./install.sh

# Configure CloudWatch Agent
CLUSTERID=$(jq '.jobFlowId' -r /emr/instance-controller/lib/info/job-flow.json)
echo '{
  "metrics": {
    "append_dimensions": {
      "ClusterID": "'${CLUSTERID}'"
    },
    "metrics_collected": {
      "statsd": {
        "metrics_collection_interval":1,
        "metrics_aggregation_interval":30
      }
    },
    "namespace": "'${CWNamespace}'/'${CLUSTERID}'"
  }
}' | sudo tee /etc/amazon/EMRCustomSparkCloudWatchSink/amazon-cloudwatch-agent-conf.json
sudo chmod 644 /etc/amazon/EMRCustomSparkCloudWatchSink/amazon-cloudwatch-agent-conf.json

# Launch CloudWatch Agent
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/etc/amazon/EMRCustomSparkCloudWatchSink/amazon-cloudwatch-agent-conf.json -s

rm -rf /tmp/EMRCustomSparkCloudWatchSink

