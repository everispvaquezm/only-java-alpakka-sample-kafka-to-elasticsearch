package samples.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.Materializer;
import akka.stream.alpakka.elasticsearch.ElasticsearchSourceSettings;
import akka.stream.alpakka.elasticsearch.ReadResult;
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class Helper {

    private static final Logger log = LoggerFactory.getLogger(Helper.class);

    private ElasticsearchContainer elasticsearchContainer;
    public String elasticsearchAddress;

    private KafkaContainer kafkaContainer;
    public String kafkaBootstrapServers;

    public Helper() {
    }

    public void startContainers() {
        elasticsearchContainer =
                new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:6.4.3");
        elasticsearchContainer.start();
        elasticsearchAddress = elasticsearchContainer.getHttpHostAddress(); //TODO maybe this could be changed to a hardcoded address like localhost:5681

        kafkaContainer = new KafkaContainer("5.1.2"); // contains Kafka 2.1.x
        kafkaContainer.start();
        kafkaBootstrapServers = kafkaContainer.getBootstrapServers(); //TODO maybe this could be changed to a hardcoded address like localhost:9092
    }

    public void stopContainers() {
        kafkaContainer.stop();
        elasticsearchContainer.stop();
    }

    CompletionStage<Done> writeToKafka(String topic, List<Movement> movements, ActorSystem actorSystem, Materializer materializer) {
        ProducerSettings<Integer, String> kafkaProducerSettings =
                ProducerSettings.create(actorSystem, new IntegerSerializer(), new StringSerializer()) //<Integer, String> due to being persisted as id and json with the object
                        .withBootstrapServers(kafkaBootstrapServers);

        CompletionStage<Done> producing =
                Source.from(movements)
                        .map(
                                movement -> {
                                    log.debug("producing {}", movement);
                                    String json = JsonMappers.movementWriter.writeValueAsString(movement);
                                    return new ProducerRecord<>(topic, movement.id, json);
                                })
                        .runWith(Producer.plainSink(kafkaProducerSettings), materializer);
        producing.thenAccept(s -> log.info("Producing finished"));
        return producing;
    }

    CompletionStage<List<Movement>> readFromElasticsearch(RestClient elasticsearchClient, String indexName, Materializer materializer) {
        CompletionStage<List<Movement>> reading =
                ElasticsearchSource.typed(
                        indexName,
                        "_doc",
                        "{\"match_all\": {}}",
                        ElasticsearchSourceSettings.create(),
                        elasticsearchClient,
                        Movement.class)
                        .map(ReadResult::source)
                        .runWith(Sink.seq(), materializer);
        reading.thenAccept(
                non -> log.info("Reading finished"));
        return reading;
    }

}
