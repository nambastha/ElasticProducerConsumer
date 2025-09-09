FROM maven:3.8.4-openjdk-11 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package

FROM openjdk:11-jre-slim
WORKDIR /app
COPY --from=build /app/target/elastic-producer-consumer-1.0-SNAPSHOT.jar .
CMD ["java", "-jar", "elastic-producer-consumer-1.0-SNAPSHOT.jar"]