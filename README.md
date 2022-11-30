# Yet Another Kafka

This project is the final component of the course UE20CS322: Big Data.

# A background to Kafka
Apache Kafka is a distributed publish-subscribe messaging system and a robust queue that can handle a high volume of data and enables you to pass messages from one end-point to another. Kafka is suitable for both offline and online message consumption. Kafka messages are persisted on the disk and replicated within the cluster to prevent data loss. Kafka is built on top of the ZooKeeper synchronization service. It integrates very well with Apache Storm and Spark for real-time streaming data analysis.

# The Aim of Our Project
In this venture, we set out to replicate the most important and basic functionality of Apache Kafka.

# High Level Architectural View
![High Level Architectural View of Kafka](https://github.com/Projects-RR-2022/BD1_283_303_305_319/blob/main/arch.png?raw=true)

# The Scope of our Project
Our implementation consists of: 

## Mini Zookeeper
* A

## Kafka Brokers
* An implementation of Kafka broker technology.
* 3 Kafka Brokers are set up.
* One Kafka broker is set as the leader, with the other two as followers.
* Broker selection is done using FCFS algorithm.
* The broker maintains and creates the topics.
* They are also responsible for registering/de-registering any Producers and Consumers.
* The leader maintains a log of all operations, which is replicated on the followers.
* In case a publish or a consume operation is encountered when the Leader has died, the remaining Kafka Brokers are able to handle this situation.

## Kafka Producers
* Dynamic number of producers.
* The Producer is be able to register to any Kafka Topic and notifies the Broker to create one if necessary.
* The producer is capable of publishing messages to any Kafka topic.
* Acknowledgement is sent from the Broker once message is transmitted - if not, the message is retransmitted.

## Kafka Consumer
* The Consumer is able to receiving messages from a Kafka Topic.
* Dynamic number of producers.
* A consumer is able to receive all the messages from the time of creation of a Kafka Topic by using the `--from-beginning` flag.
  * When the flag is used, consumer obtains all the messages since the creation of the topic.
  * If the flag isn't used, consumer obtains messages only sent after subscription to the topic.


Team Members:

* Prateek Rao: PES1UG20CS303
* Rahul Ramesh: PES1UG20CS319
* Pooshpal Baheti: PES1UG20CS283
* Pratham Bhat: PES1UG20CS305

