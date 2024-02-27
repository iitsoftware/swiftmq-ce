FROM ghcr.io/graalvm/jdk-community:21
RUN microdnf install -y rsync gzip
COPY swiftmq.tar.gz /swiftmq.tar.gz
WORKDIR /
RUN tar xvfz swiftmq.tar.gz
RUN mkdir -p /swiftmq/preconfig mkdir /swiftmq/initialconfig mkdir /swiftmq/initialpreconfig
COPY /swiftmq/data/config/routerconfig.xml /swiftmq/initialconfig/.
COPY /swiftmq/data/preconfig/* /swiftmq/initialpreconfig/.
ENV SWIFTMQ_INITIAL_CONFIG /swiftmq/initialconfig/routerconfig.xml
ENV SWIFTMQ_IN_DOCKER true
WORKDIR /swiftmq/scripts
RUN echo $JAVA_HOME > .javahome && echo $JAVA_HOME/bin > .executables
CMD ./checkconfig && exec ./router $SWIFTMQ_PRECONFIG


