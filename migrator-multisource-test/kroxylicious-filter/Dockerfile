# Kroxylicious proxy image with the ConsumerGroupPrefix filter on its classpath, via the
# "classpath-plugins" extension point (same approach as kroxy-linking-demo's topic-prefixer).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B -q package -DskipTests

FROM quay.io/kroxylicious/proxy:0.24.0
COPY --from=build /build/target/consumer-group-prefix.jar /opt/kroxylicious/classpath-plugins/consumer-group-prefix/consumer-group-prefix.jar
# Stock 0.24.0 log4j2.yaml plus a logger for the filter, so rewrites can be logged at DEBUG
# without raising the whole proxy's log level.
COPY log4j2.yaml /opt/kroxylicious/config/log4j2.yaml
