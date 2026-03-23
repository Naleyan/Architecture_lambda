#!/bin/bash

if [ "$SPARK_WORKLOAD" == "master" ]; then
  echo "Starting Spark Master..."
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master

elif [ "$SPARK_WORKLOAD" == "worker" ]; then
  echo "Starting Spark Worker..."
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER

else
  echo "No workload specified"
fi
