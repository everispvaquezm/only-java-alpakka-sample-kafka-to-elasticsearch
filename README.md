This Java code comes from https://akka.io/alpakka-samples/kafka-to-elasticsearch/ and https://akka.io/alpakka-samples/kafka-to-elasticsearch/step_001_complete.html

To execute this little demo:

1) Start your docker local installation
2) Go to the directory where this README.md file is located an issue the following command: 

mvn clean compile exec:java -Dexec.mainClass=samples.javadsl.Main

At the end of the execution is throwing an Exception but I think is reading from Kafka and writing to Elasticsearch ...
