FROM eclipse-temurin:17-jdk-jammy

WORKDIR /app

COPY container.jar garmin.jar
COPY application.properties application.properties

EXPOSE 8080

# Usa Spring Boot con configurazione esterna
CMD ["java", "-jar", "garmin.jar", "--spring.config.location=file:/app/application.properties"]
