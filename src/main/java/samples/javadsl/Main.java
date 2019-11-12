/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package samples.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.Terminated;
import akka.japi.Pair;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.elasticsearch.ElasticsearchWriteSettings;
import akka.stream.alpakka.elasticsearch.WriteMessage;
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchFlow;
import akka.stream.javadsl.Keep;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

// #imports
// #imports

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  private final Helper helper;

  private final String topic = "movements-to-elasticsearch";

  // #es-setup
  private final String indexName = "movements";

  // #es-setup

  public Main(Helper helper) {
    this.helper = helper;
  }

  private ActorSystem actorSystem;
  private Materializer materializer;
  private RestClient elasticsearchClient;

  private Consumer.DrainingControl<Done> readFromKafkaToEleasticsearch() {
    // #kafka-setup
    // configure Kafka consumer (1)
    String groupId = "docs-group";
    ConsumerSettings<Integer, String> kafkaConsumerSettings =
      ConsumerSettings.create(actorSystem, new IntegerDeserializer(), new StringDeserializer())
        .withGroupId(groupId)
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withStopTimeout(Duration.ofSeconds(5));
    // #kafka-setup

    // #flow
    // #flow
    return Consumer.committableSource(kafkaConsumerSettings, Subscriptions.topics(topic)) // (5)
      .asSourceWithContext(ConsumerMessage.CommittableMessage::committableOffset) // (6)
      .map(ConsumerMessage.CommittableMessage::record)
      .map(
        consumerRecord -> { // (7)
          Movement movement = JsonMappers.movementReader.readValue(consumerRecord.value());
          return WriteMessage.createUpsertMessage(String.valueOf(movement.iban), movement);
        })
      .via(
        ElasticsearchFlow.createWithContext(
          indexName,
          "_doc",
          ElasticsearchWriteSettings.create(),
          elasticsearchClient,
          JsonMappers.mapper)) // (8)
      .map(
        writeResult -> { // (9)
          writeResult
            .getError()
            .ifPresent(
              errorJson -> {
                throw new RuntimeException(
                  "Elasticsearch update failed "
                    + writeResult.getErrorReason().orElse(errorJson));
              });
          return NotUsed.notUsed();
        })
      .asSource() // (10)
      .map(Pair::second)
      .toMat(Committer.sink(CommitterSettings.create(actorSystem)), Keep.both()) // (11)
      .mapMaterializedValue(Consumer::createDrainingControl) // (12)
      .run(materializer);
  }

  private CompletionStage<Terminated> run() throws Exception {
    actorSystem = ActorSystem.create(); //ActorSystem is the only consumer of configuration information
    materializer = ActorMaterializer.create(actorSystem);
    // #es-setup
    // Elasticsearch client setup (4)
    //TODO change this ugly way of retrieving elasticsearch host:port nad let the ActorSystem load the configuration by itself
    String elasticsearchAddress = actorSystem.settings().config().getConfig("akka.elasticsearch").getValue("network.host").render().replace("\"", "");
    elasticsearchClient = RestClient.builder(HttpHost.create(elasticsearchAddress)).build();

    // #es-setup

    List<Movement> movements = Arrays.asList(
      new Movement((int) (Math.random() * 100) + 1, UUID.randomUUID().toString(), 100d),
      new Movement((int) (Math.random() * 100) + 1, UUID.randomUUID().toString(), 300d));
    CompletionStage<Done> writing = helper.writeToKafka(topic, movements, actorSystem, materializer);
    writing.toCompletableFuture().get(10, TimeUnit.SECONDS);

    Consumer.DrainingControl<Done> control = readFromKafkaToEleasticsearch();
    TimeUnit.SECONDS.sleep(5);
    CompletionStage<Done> copyingFinished = control.drainAndShutdown(actorSystem.dispatcher());
    copyingFinished.toCompletableFuture().get(10, TimeUnit.SECONDS);
    CompletionStage<List<Movement>> reading = helper.readFromElasticsearch(elasticsearchClient, indexName, materializer);

    return reading.thenCompose(
      ms -> {
        ms.forEach(m -> System.out.println("An account movement has been read:" + m));
        try {
          elasticsearchClient.close();
        } catch (IOException e) {
          log.error(e.toString(), e);
        }
        actorSystem.terminate();
        return actorSystem.getWhenTerminated();
      });
  }

  public static void main(String[] args) throws Exception {
    Helper helper = new Helper();
    Main main = new Main(helper);
    CompletionStage<Terminated> run = main.run();
  }
}
