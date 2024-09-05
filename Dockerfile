FROM jfrog.fkinternal.com/docker-external/openjdk:17-oracle

WORKDIR /app
COPY target/topN-1.0-SNAPSHOT.jar /app/topN-application.jar

RUN chmod +x /app/topN-application.jar

EXPOSE 8080

CMD ["java", "-jar", "/app/topN-application.jar"]


