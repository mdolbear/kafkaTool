# kafkaTool
This project is a kafka debug tool that gives one the ability to listen on a topic or post to a topic for debugging.

#Initial way to start things:

*Run this to start kafka in your env. You need to have docker installed:

docker run -d --name pause -p 9092:9092 -p 2181:2181 gcr.io/google_containers/pause-amd64:3.0

docker run -d --net=container:pause --ipc=container:pause --pid=container:pause -e "ZOOKEEPER_CLIENT_PORT=2181" confluentinc/cp-zookeeper:4.0.0-3

docker run -d --net=container:pause --ipc=container:pause --pid=container:pause -e "KAFKA_ZOOKEEPER_CONNECT=localhost:2181" -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092" -e "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1" confluentinc/cp-kafka:4.0.0-3


*Start kafka tool. I am currently starting it in IntelliJ:

Run the following command:

curl -d "name=mysub&bootstrapServers=localhost:9092&clientId=clientId&groupId=mygroup&desKeyClassShortName=String&desValueClassShortName=EventObject&topic=game-events" -X POST http://localhost:8080/kafkatool/subscribe
curl -X DELETE http://localhost:8080/kafkatool/removeSubscriber?name=mysub


This creates a subscription. Now you need to create events on this topic, and you should see some logs from the consumer of the topic. 
Currently, this is just getting blank lines. I need to fix this.