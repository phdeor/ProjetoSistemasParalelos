FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /app

COPY pom.xml .
RUN mvn dependency:go-offline

COPY src ./src

RUN mvn clean package -DskipTests


FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY --from=builder /app/target/bbs-chat-1.0-SNAPSHOT.jar app.jar

CMD ["java", "-cp", "app.jar", "com.sistema.ServerMain"]