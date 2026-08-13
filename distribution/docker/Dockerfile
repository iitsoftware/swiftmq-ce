FROM ghcr.io/graalvm/graalvm-community:25.0.1

ADD swiftmq.tar.gz /

RUN mkdir -p \
    /swiftmq/preconfig \
    /swiftmq/initialconfig \
    /swiftmq/initialpreconfig

COPY swiftmq/data/config/routerconfig.xml /swiftmq/initialconfig/
COPY swiftmq/data/preconfig/ /swiftmq/initialpreconfig/

ENV SWIFTMQ_INITIAL_CONFIG=/swiftmq/initialconfig/routerconfig.xml
ENV SWIFTMQ_IN_DOCKER=true

WORKDIR /swiftmq/scripts

RUN echo "$JAVA_HOME" > .javahome \
 && echo "$JAVA_HOME/bin" > .executables

CMD ./checkconfig && exec ./router $SWIFTMQ_PRECONFIG

