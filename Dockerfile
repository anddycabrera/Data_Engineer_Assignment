FROM bitnami/spark
### need if using the recommended Bitnami base image
USER root

### Make sure wget is available
RUN apt-get update && apt-get install -y wget && apt install curl -y && rm -r /var/lib/apt/lists /var/cache/apt/archives

### Modify the Hadoop and Spark versions below as needed.
### NOTE: The HADOOP_HOME and SPARK_HOME locations should not be modified
ENV HADOOP_VERSION=3.3.1
ENV HADOOP_HOME=/opt/bitnami/hadoop
ENV HADOOP_CONF_DIR=/opt/bitnami/hadoop/etc/hadoop
ENV SPARK_VERSION=3.1.1
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH="$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin"

ENV PYTHONPATH="$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"

### Enable access to AWS and ADLS Gen2. Can modify as needed
ENV HADOOP_OPTIONAL_TOOLS="hadoop-aws"

### Remove the pre-installed Spark since it is pre-bundled with hadoop but preserve the python env
WORKDIR /opt/bitnami
RUN [ -d ${SPARK_HOME}/venv ] && mv ${SPARK_HOME}/venv /opt/bitnami/temp-venv
RUN rm -rf ${SPARK_HOME}

### Install the desired Hadoop-free Spark distribution
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz && \
    tar -xf spark-${SPARK_VERSION}-bin-without-hadoop.tgz && \
    rm spark-${SPARK_VERSION}-bin-without-hadoop.tgz && \
    mv spark-${SPARK_VERSION}-bin-without-hadoop ${SPARK_HOME} && \
    chmod -R 777 ${SPARK_HOME}/conf

### Restore the virtual python environment
RUN [ -d /opt/bitnami/temp-venv ] && mv /opt/bitnami/temp-venv ${SPARK_HOME}/venv

### Install the desired Hadoop libraries
RUN wget -q http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xf hadoop-${HADOOP_VERSION}.tar.gz && \
    rm hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

### Setup the Hadoop libraries classpath
RUN echo 'export SPARK_DIST_CLASSPATH="$(hadoop classpath):'"${HADOOP_HOME}"'/share/hadoop/tools/lib/*"' >> ${SPARK_HOME}/conf/spark-env.sh
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:$HADOOP_HOME/lib/native"

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.901.jar

RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar --output /opt/bitnami/spark/jars/hadoop-aws-3.3.1.jar
         

### This is important to maintain compatibility with Bitnami
RUN mkdir -p /assignment
WORKDIR /assignment
RUN pip install --upgrade pip && pip install boto3 && pip install jupyter
RUN pip install moto[server]

COPY Data_Engineer_Assignment.ipynb /assignment

RUN /opt/bitnami/scripts/spark/postunpack.sh
RUN spark-shell --packages com.amazonaws:aws-java-sdk:1.11.901,org.apache.hadoop:hadoop-aws:3.3.1
CMD ["jupyter", "notebook", "--no-browser", "--ip=0.0.0.0", "--allow-root", "--NotebookApp.token=''"] 

EXPOSE 8080
EXPOSE 7077

