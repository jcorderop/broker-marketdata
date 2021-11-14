# Docker example using fatjar
# - mvn clean install
# - docker build . -t jcorderop/broker-marketdata
# - docker run -e SPRING_PROFILES_ACTIVE='docker' -t -i -p 8900:8900 jcorderop/broker-marketdata
# - docker push docker.io/jcorderop/broker-marketdata:latest
# - cd .\docker-compose\marketdata\
# - cd .\docker-compose\postgres\
# - docker-compose up -d

# https://hub.docker.com/
FROM openjdk:17-slim

#Information around who maintains the image
MAINTAINER jc

# Add the application's jar to the container
COPY target/broker-marketdata-1.0.1-SNAPSHOT.jar broker-marketdata-1.0.1-SNAPSHOT.jar

#execute the application
ENTRYPOINT ["java","-jar","/broker-marketdata-1.0.1-SNAPSHOT.jar"]
