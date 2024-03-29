#!/bin/bash

cd /home/javaapps/sbt-projects/SparkCovid/ || exit

/usr/local/spark-3.4.0-bin-hadoop3-scala2.13/bin/spark-submit --class sct.SparkCovidWho --master local[4] target/scala-2.13/SparkCovid-assembly-0.1.0-SNAPSHOT.jar

result="$?"

cd - || exit

if [ "$result" -ne 0 ]; then
  exit 1
fi

exit 0