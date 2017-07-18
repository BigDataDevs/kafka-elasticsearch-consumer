package org.elasticsearch.kafka.indexer.jobs;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author marinapopova
 *         Apr 14, 2016
 */
public class ConsumerManager {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerManager.class);
    private static final String KAFKA_CONSUMER_THREAD_NAME_FORMAT = "kafka-elasticsearch-consumer-thread-%d";

    @Value("${kafka.consumer.source.topic:testTopic}")
    private String kafkaTopic;
    @Value("${kafka.consumer.group.name:kafka-elasticsearch-consumer}")
    private String consumerGroupName;
    @Value("${application.id:instance1}")
    private String consumerInstanceName;
    @Value("${kafka.consumer.brokers.list:localhost:9092}")
    private String kafkaBrokersList;
    @Value("${kafka.consumer.session.timeout.ms:10000}")
    private int consumerSessionTimeoutMs;
    // interval in MS to poll Kafka brokers for messages, in case there were no messages during the previous interval
    @Value("${kafka.consumer.poll.interval.ms:10000}")
    private long kafkaPollIntervalMs;
    // Max number of bytes to fetch in one poll request PER partition
    // default is 1M = 1048576
    @Value("${kafka.consumer.max.partition.fetch.bytes:1048576}")
    private int maxPartitionFetchBytes;
    // if set to TRUE - enable logging timings of the event processing
    // TODO add implementation to use this flag
    @Value("${is.perf.reporting.enabled:false}")
    private boolean isPerfReportingEnabled;

    @Value("${kafka.consumer.pool.count:3}")
    private int kafkaConsumerPoolCount;

    @Autowired
    @Qualifier("messageHandler")
    private ObjectFactory<IMessageHandler> messageHandlerObjectFactory;

    private String consumerStartOptionsConfig;

    private ExecutorService consumersThreadPool = null;
    private List<ConsumerWorker> consumers = new ArrayList<>();
    private Properties kafkaProperties;
    private Map<Integer, ConsumerStartOption> consumerStartOptions;

    private AtomicBoolean running = new AtomicBoolean(false);

    public ConsumerManager() {
    }



    public void setConsumerStartOptionsConfig(String consumerStartOptionsConfig) {
        this.consumerStartOptionsConfig = consumerStartOptionsConfig;
    }

    private void init() {
        logger.info("init() is starting ....");

        kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokersList);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName);
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, consumerSessionTimeoutMs);
        kafkaProperties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // TODO make a dynamic property determined from the mockedKafkaCluster metadata
        int consumerPoolCount = kafkaConsumerPoolCount;
        consumerStartOptions = ConsumerStartOption.fromFile(consumerStartOptionsConfig);
        determineOffsetForAllPartitionsAndSeek();
        initConsumers(consumerPoolCount);
    }

    private void initConsumers(int consumerPoolCount) {
        logger.info("initConsumers() started, consumerPoolCount={}", consumerPoolCount);
        consumers = new ArrayList<>();
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(KAFKA_CONSUMER_THREAD_NAME_FORMAT).build();
        consumersThreadPool = Executors.newFixedThreadPool(consumerPoolCount, threadFactory);

        for (int consumerNumber = 0; consumerNumber < consumerPoolCount; consumerNumber++) {
            ConsumerWorker consumer = new ConsumerWorker(
                    consumerNumber, consumerInstanceName, kafkaTopic, kafkaProperties, kafkaPollIntervalMs, messageHandlerObjectFactory.getObject());
            consumers.add(consumer);
            consumersThreadPool.submit(consumer);
        }
    }

    private void shutdownConsumers() {
        logger.info("shutdownConsumers() started ....");

        if (consumers != null) {
            for (ConsumerWorker consumer : consumers) {
                consumer.shutdown();
            }
        }
        if (consumersThreadPool != null) {
            consumersThreadPool.shutdown();
            try {
                consumersThreadPool.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warn("Got InterruptedException while shutting down consumers, aborting");
            }
        }
        if (consumers != null) {
            consumers.forEach(consumer -> consumer.getPartitionOffsetMap()
                    .forEach((topicPartition, offset)
                            -> logger.info("Offset position during the shutdown for consumerId : {}, partition : {}, offset : {}", consumer.getConsumerId(), topicPartition.partition(), offset.offset())));
        }
        logger.info("shutdownConsumers() finished");


    }

    private void determineOffsetForAllPartitionsAndSeek() {
        KafkaConsumer consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(Arrays.asList(kafkaTopic));

        //Make init poll to get assigned partitions
        consumer.poll(kafkaPollIntervalMs);
        Set<TopicPartition> assignedTopicPartitions = consumer.assignment();

        //apply start offset options to partitions specified in 'consumer-start-options.config' file
        for (TopicPartition topicPartition : assignedTopicPartitions) {
            ConsumerStartOption startOption = consumerStartOptions.get(topicPartition.partition());
            long offsetBeforeSeek = consumer.position(topicPartition);
            if (startOption == null) {
                startOption = consumerStartOptions.get(ConsumerStartOption.DEFAULT);
            }
            switch (startOption.getStartFrom()) {
                case CUSTOM:
                    consumer.seek(topicPartition, startOption.getStartOffset());
                    break;
                case EARLIEST:
                    consumer.seekToBeginning(Arrays.asList(topicPartition));
                    break;
                case LATEST:
                    consumer.seekToEnd(Arrays.asList(topicPartition));
                    break;
                case RESTART:
                default:
                    break;
            }
            logger.info("Offset for partition: {} is moved from : {} to {}", topicPartition.partition(), offsetBeforeSeek, consumer.position(topicPartition));
            logger.info("Offset position during the startup for consumerId : {}, partition : {}, offset : {}", Thread.currentThread().getName(), topicPartition.partition(), consumer.position(topicPartition));
        }
        consumer.commitSync();
        consumer.close();
    }

    @PostConstruct
    public void postConstruct() {

        start();
    }

    @PreDestroy
    public void preDestroy() {

        stop();
    }

    synchronized public void start() {
        if (!running.getAndSet(true)) {
            init();
        } else {
            logger.warn("Already running");
        }
    }

    synchronized public void stop() {
        if (running.getAndSet(false)) {
            shutdownConsumers();
        } else {
            logger.warn("Already stopped");
        }
    }

}
