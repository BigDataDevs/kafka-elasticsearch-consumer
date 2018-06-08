package org.elasticsearch.kafka.indexer.jobs;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
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
import javax.annotation.Resource;
import java.util.*;
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
    public static final String PROPERTY_SEPARATOR = ".";

    @Value("${kafka.consumer.source.topic:testTopic}")
    private String kafkaTopic;
    @Value("${kafka.consumer.group.name:kafka-elasticsearch-consumer}")
    private String consumerGroupName;
    @Value("${application.id:app1}")
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
    @Value("${is.perf.reporting.enabled:false}")
    private boolean isPerfReportingEnabled;

    @Value("${kafka.consumer.pool.count:3}")
    private int kafkaConsumerPoolCount;

    @Autowired
    @Qualifier("messageHandler")
    private ObjectFactory<IMessageHandler> messageHandlerObjectFactory;

    @Resource(name = "applicationProperties")
    private Properties applicationProperties;

    @Value("${kafka.consumer.property.prefix:consumer.kafka.property.}")
    private String consumerKafkaPropertyPrefix;

    private String consumerStartOptionsConfig;

    private ExecutorService consumersThreadPool = null;
    private List<ConsumerWorker> consumers = new ArrayList<>();
    private Properties kafkaProperties;

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

        consumerKafkaPropertyPrefix = consumerKafkaPropertyPrefix.endsWith(PROPERTY_SEPARATOR) ? consumerKafkaPropertyPrefix : consumerKafkaPropertyPrefix + PROPERTY_SEPARATOR;
        extractAndSetKafkaProperties(applicationProperties, kafkaProperties, consumerKafkaPropertyPrefix);
        int consumerPoolCount = kafkaConsumerPoolCount;
        Map<Integer, ConsumerStartOption> consumerStartOptions = ConsumerStartOption.fromConfig(consumerStartOptionsConfig);
        determineOffsetForAllPartitionsAndSeek(consumerStartOptions, null);
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

    public void determineOffsetForAllPartitionsAndSeek(Map<Integer, ConsumerStartOption> consumerStartOptions, Consumer<String, String> consumer) {
        logger.info("in determineOffsetForAllPartitionsAndSeek(): ");
        if (consumerStartOptions.isEmpty() || consumerStartOptions.values().contains(ConsumerStartOption.RESTART_OPTION)) {
        	logger.info("consumerStartOptions is empty or set to RESTART - consumers will start from RESTART for all partitions" );
        	return;
        }

        if (consumer == null) {
            consumer = new KafkaConsumer<>(kafkaProperties);
        }
        consumer.subscribe(Arrays.asList(kafkaTopic));

        //Make init poll to get assigned partitions
        consumer.poll(kafkaPollIntervalMs);

        Set<TopicPartition> assignedTopicPartitions = consumer.assignment();
        Map<TopicPartition, Long> offsetsBeforeSeek = new HashMap<>();
        for (TopicPartition topicPartition : assignedTopicPartitions) {
            offsetsBeforeSeek.put(topicPartition, consumer.position(topicPartition));
        }

        ConsumerStartOption defaultStartOption = consumerStartOptions.get(ConsumerStartOption.ALL_PARTITIONS);
        if (defaultStartOption == null) {
            //apply custom start offset options to partitions from file
            if (consumerStartOptions.size() == assignedTopicPartitions.size()) {
                for (TopicPartition topicPartition : assignedTopicPartitions) {
                    ConsumerStartOption startOption = consumerStartOptions.get(topicPartition.partition());
                    if (startOption == null) {
                        logger.error("There is no custom start option for partition {}. Consumers will start from RESTART for all partitions", topicPartition.partition());
                        consumer.close();
                        return;
                    }

                    consumer.seek(topicPartition, startOption.getStartOffset());
                }

                consumer.commitSync();
                for (TopicPartition topicPartition : assignedTopicPartitions) {
                    logger.info("Offset for partition: {} is moved from : {} to {}",
                            topicPartition.partition(), offsetsBeforeSeek.get(topicPartition), consumer.position(topicPartition));
                }
            } else {
                logger.error("Defined custom consumer start options has missed partitions. Expected {} partitions but was defined {}. Consumers will start from RESTART for all partitions",
                        assignedTopicPartitions.size(), consumerStartOptions.size());
            }
        } else {
            // handling LATEST, EARLIEST, or CUSTOM options for all partitions
            switch (defaultStartOption.getStartFrom()) {
                case CUSTOM:
                    for (TopicPartition topicPartition : assignedTopicPartitions) {
                        consumer.seek(topicPartition, defaultStartOption.getStartOffset());
                    }
                    break;
                case EARLIEST:
                    consumer.seekToBeginning(assignedTopicPartitions);
                    break;
                case LATEST:
                    consumer.seekToEnd(assignedTopicPartitions);
                    break;
                default:
                    break;
            }
            consumer.commitSync();
            for (TopicPartition topicPartition : assignedTopicPartitions) {
                logger.info("Offset for partition: {} is moved from : {} to {}",
                        topicPartition.partition(), offsetsBeforeSeek.get(topicPartition), consumer.position(topicPartition));
            }
        }
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

    public static void extractAndSetKafkaProperties(Properties applicationProperties, Properties kafkaProperties, String kafkaPropertyPrefix) {
        if(applicationProperties != null && applicationProperties.size() >0){
            for(Map.Entry <Object,Object> currentPropertyEntry :applicationProperties.entrySet()){
                String propertyName = currentPropertyEntry.getKey().toString();
                if(StringUtils.isNotBlank(propertyName) && propertyName.contains(kafkaPropertyPrefix)){
                    String validKafkaConsumerProperty = propertyName.replace(kafkaPropertyPrefix, StringUtils.EMPTY);
                    kafkaProperties.put(validKafkaConsumerProperty, currentPropertyEntry.getValue());
                    logger.info("Adding kafka property with prefix: {}, key: {}, value: {}", kafkaPropertyPrefix, validKafkaConsumerProperty, currentPropertyEntry.getValue());
                }
            }
        }
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public long getKafkaPollIntervalMs() {
        return kafkaPollIntervalMs;
    }

    public void setKafkaPollIntervalMs(long kafkaPollIntervalMs) {
        this.kafkaPollIntervalMs = kafkaPollIntervalMs;
    }
}
