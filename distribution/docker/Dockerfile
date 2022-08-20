FROM ghcr.io/graalvm/graalvm-ce:22.2.0
RUN gu install js
RUN microdnf install rsync
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


