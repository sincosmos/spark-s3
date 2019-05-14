#!/usr/bin/env bash

spark-submit \
--class com.jd.ads.n9jdcloud.JDCloudSparkWriter \
--master yarn \
--conf spark.yarn.maxAppAttempts=1 \
--deploy-mode cluster \
--name "JDCloudSparkWriter" \
--queue root.bdp_jmart_ad.jd_ad_dmp \
--driver-memory 15G \
--driver-cores 3 \
--conf spark.driver.maxResultSize=10g \
--num-executors 60 \
--executor-cores 6 \
--executor-memory 20G \
--conf spark.executor.memoryOverhead=9216 \
--conf spark.storage.memoryFraction=0.6 \
--conf spark.shuffle.memoryFraction=0.4 \
--conf spark.sql.shuffle.partitions=1000 \
--conf spark.sql.broadcastTimeout=800 \
--conf spark.default.parallelism=1000 \
--conf spark.rpc.message.maxSize=200 \
--conf spark.core.connection.ack.wait.timeout=800 \
--conf spark.sql.codegen.wholeStage=false \
n9-jdcloud-1.0-SNAPSHOT.jar

