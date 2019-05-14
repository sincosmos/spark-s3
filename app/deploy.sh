#!/usr/bin/env bash

export JAVA_HOME=/software/servers/jdk1.8.0_121
# copy properties files
cd ../
/home/ads_model/mining/maven/bin/mvn clean
/home/ads_model/mining/maven/bin/mvn package
cd -

# spark writer
/bin/rm -rf spark-s3-writer
mkdir spark-s3-writer
cp spark-submit.sh ./spark-s3-writer
cp ../target/n9-jdcloud-1.0-SNAPSHOT-jar-with-dependencies.jar ./spark-s3-writer