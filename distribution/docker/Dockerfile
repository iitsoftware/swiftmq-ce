FROM ghcr.io/graalvm/graalvm-ce:latest
RUN apt-get update && apt-get upgrade -y && apt-get install -y rsync git
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
COPY swiftmq.tar.gz /swiftmq.tar.gz
WORKDIR /
RUN tar xvfz swiftmq.tar.gz
RUN mkdir /swiftmq/preconfig
RUN mkdir /swiftmq/initialconfig
RUN mkdir /swiftmq/initialpreconfig
COPY /swiftmq/data/config/routerconfig.xml /swiftmq/initialconfig/.
COPY /swiftmq/data/preconfig/* /swiftmq/initialpreconfig/.
ENV SWIFTMQ_INITIAL_CONFIG /swiftmq/initialconfig/routerconfig.xml
WORKDIR /swiftmq/scripts
CMD ./checkconfig && exec ./router $SWIFTMQ_PRECONFIG


