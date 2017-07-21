# Data Cleaner

## Introduce
This is a spark application for data clean.

## Pre-required
Apache Spark 1.6.0(YARN based)
Apache Maven

## Compile & Package
```shell
mvn package
```
The jar package will be generated under directory named target.

## Deploy
```shell
spark-submit --master yarn-cluster --files /path/to/your/config.properties /path/to/your/DataCleaner.jar
```
Note: Make sure that your account has the right permission.

