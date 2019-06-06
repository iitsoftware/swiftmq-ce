FROM openjdk:8-jre-alpine
RUN apk update && apk add bash
COPY swiftmq.tar.gz /swiftmq.tar.gz
WORKDIR /
RUN tar xvfz swiftmq.tar.gz
RUN mkdir /swiftmq/preconfig
RUN mkdir /swiftmq/initialconfig
COPY /swiftmq/data/config/routerconfig.xml /swiftmq/initialconfig/.
WORKDIR /swiftmq/scripts
ENV SWIFTMQ_INITIAL_CONFIG /swiftmq/initialconfig/routerconfig.xml
ENV SWIFTMQ_PRECONFIG none
CMD ./checkconfig && exec ./router $SWIFTMQ_PRECONFIG $SWIFTMQ_INITIAL_CONFIG

