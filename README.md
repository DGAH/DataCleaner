# Data Cleaner

## Introduce
This is a spark application for data clean.

## Pre-required
Apache Spark 2.2.0(YARN based)
Apache Maven

## Compile & Package
```shell
mvn package
```
The jar package will be generated under directory named target.

## Deploy
```shell
SPARK_KAFKA_VERSION=0.10 spark2-submit --master yarn --deploy-mode cluster --files /path/to/your/config.properties /path/to/your/DataCleaner.jar
```
Note: Make sure that your account has the right permission.

