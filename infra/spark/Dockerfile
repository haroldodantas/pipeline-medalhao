
FROM quay.io/jupyter/pyspark-notebook:x86_64-spark-3.5.3

USER root

RUN curl -fsSL -o /usr/local/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o /usr/local/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

USER ${NB_UID}
