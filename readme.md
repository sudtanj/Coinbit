# Coinbit

## Description
Make a service where user can deposit money into their wallet and fetch their current balance using Event-driven  architecture design. 
Required implementations 
1.  HTTP POST endpoint for user to deposit money with this payload: 
    * wallet_id 
    * amount: float 
2. HTTP GET endpoint to get the balance of the wallet, and also a flag whether the wallet has ever done one  or more deposits with amounts more than 10,000 within a single 2-minute window (rolling-period). The  endpoint should return this response: 
    * wallet_id 
    * balance: float 
    * above_threshold: boolean 
        * Test Cases: 
            * True if two deposits of 6,000 amount each, both within 2 minutes. 
            * False if one single deposit of 6,000, then after 2-minutes later another single deposit of 6,000. 
            * False if five deposits of 2,000 amount each all within 2 minutes, then after 5 seconds later  another single deposit of 6,000. 
            * True if six deposits of 2,000 amount each all within 2 minutes. 

## High-level diagram using Goka
![img](https://i.ibb.co/LhMhvSD/image.png)

## Architecture requirement
* Use [Goka](https://github.com/lovoo/goka) to build the service above. 
* Use [protobuf](https://developers.google.com/protocol-buffers/docs/gotutorial) when encoding/decoding payload to/from Kafka broker. 
* Use Goka's Local storage mechanism, if a database is required. 

## Setting up Apache Kafka 
We’ve provided a `docker-compose.yml` file to easily deploy Kafka brokerin yourlocal machine.This assumes  that you have a Docker engine installed in your machine. If not, please install Dockerfirst. 
```yaml
version: "3"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.0.1
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
  broker:
    image: confluentinc/cp-kafka:7.0.1
    container_name: broker
    ports:
# To learn about configuring Kafka for access across networks see
#<https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/>
      - "9092:9092"
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: "zookeeper:2181"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://broker:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

```


## How to run the program 
You need to run docker container first using the following command
```
docker-compose up
```
before start the program using the following command
```
go run main.go
```

## Postman Documentation API
https://documenter.getpostman.com/view/24756256/2s8YzZNyYj

## Known Issues
- Sometimes the program need to be restart 2 / 3 times if the topic has not been created yet.
