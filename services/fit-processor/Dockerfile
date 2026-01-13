# Java image
FROM  eclipse-temurin:17-jdk-jammy

WORKDIR /app

COPY container-2.11.jar  garmin.jar

EXPOSE 8080

CMD ["java", "-jar", "garmin.jar"]
