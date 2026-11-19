FROM eclipse-temurin:25-jre
ARG SERVICE
WORKDIR /app
COPY ${SERVICE}/build/libs/${SERVICE}.jar /app/service.jar
USER 10001:10001
EXPOSE 8082 8083
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-Djava.awt.headless=true", "-jar", "/app/service.jar"]
