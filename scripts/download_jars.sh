#!/bin/bash
# Download required JAR dependencies for Spark

echo "Downloading JARs..."

mkdir -p jars

curl -L -o jars/postgresql-42.7.1.jar \
  https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

curl -L -o jars/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

curl -L -o jars/aws-java-sdk-bundle-1.12.262.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

echo "Done! JARs downloaded to jars/"
ls -lh jars/*.jar