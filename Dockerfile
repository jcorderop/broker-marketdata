# Docker example using fatjar
# - mvn clean install
# - docker build . -t jcorderop/market-data-service
# - docker run -t -i -p 8900:8900 jcorderop/market-data-service
# - docker push docker.io/jcorderop/market-data-service:latest
# - cd .\docker-compose\marketdata\
# - cd .\docker-compose\postgres\
# - docker-compose up -d

# https://hub.docker.com/
FROM openjdk:17-slim

ENV FAT_JAR broker-marketdata-1.0.0-SNAPSHOT-fat.jar
ENV APP_HOME /usr/app

MAINTAINER jc

EXPOSE 8900

COPY target/$FAT_JAR $APP_HOME/

WORKDIR $APP_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["exec java -jar $FAT_JAR"]
