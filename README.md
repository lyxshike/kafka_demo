# kafka_demo


kafka， partition之间有什么关系呢？

同一个messagegroup中的consumer，不能消费同一个分区里面的数据
        一个消费者组只维护了一个partition offset。

我不明白partition和topic之间的关系


kafka-console-producer.bat --broker-list localhost:9092 --topic first

kafka-topics.bat --zookeeper localhost:2181 --create --topic second --partitions 3 --replication-factor 2


kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic first --from-beginning

消费者组中的消费者个数 一般要和  partition的个数相等的
