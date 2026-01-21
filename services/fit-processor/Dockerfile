FROM eclipse-temurin:17-jdk-jammy

WORKDIR /app

COPY container.jar garmin.jar

EXPOSE 8080

CMD ["java", "-jar", "garmin.jar"]