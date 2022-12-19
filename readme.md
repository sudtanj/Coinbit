# Wallet Kafka

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