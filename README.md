# sparkstreaming-kafka
## 工作流为：flume-kafka-sparkstreaming-hbase

### 启动顺序及命令行
#### hadoop
```
cd /usr/local/src/java/hadoop/hadoop-2.6.0
sbin/start-all.sh
sbin/stop-all.sh
```
#### kafka
```
cd /usr/local/src/kafka/kafka_2.10-0.10.0.0
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test3
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test3 --from-beginning
```
#### hbase
```
cd /usr/local/src/hbase/hbase-0.96.2-hadoop2
bin/start-hbase.sh
bin/hbase shell
hbase>status
```
#### spark
```
cd /usr/local/src/spark/spark-1.6.1-bin-hadoop2.6
./bin/spark-submit --class mytest.test1.App /usr/local/src/testdatas/test1-0.0.1-SNAPSHOT-shaded.jar
./bin/spark-submit --class mytest.test1.testHbase /usr/local/src/testdatas/uber-test1-0.0.1-SNAPSHOT.jar
spark.driver.extraClassPath /usr/local/src/hbase/hbase-0.96.2-hadoop2/lib/hbase-protocol-0.96.2-hadoop2.jar
spark.executor.extraClassPath /usr/local/src/hbase/hbase-0.96.2-hadoop2/lib/hbase-protocol-0.96.2-hadoop2.jar
```
#### flume
```
cd /usr/local/src/flume/flume-1.8.0/conf
../bin/flume-ng agent --conf conf --conf-file morphline-flume.conf --name a1 Dflume.root.logger=DEBUG,console
```
