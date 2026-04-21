# Build the Flink job jar in an isolated Maven image.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml ./
# Pre-fetch deps to leverage layer cache.
RUN mvn -B -q -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -B -DskipTests package

# The job artifact is published to an Nginx-less tiny image so it can be
# mounted into the Flink JobManager container (see docker-compose).
FROM alpine:3.19
RUN mkdir -p /opt/flink/usrlib
COPY --from=build /src/target/flink-aggregator-*.jar /opt/flink/usrlib/flink-aggregator.jar
CMD ["sh","-c","echo 'job jar at /opt/flink/usrlib/flink-aggregator.jar'; sleep infinity"]
