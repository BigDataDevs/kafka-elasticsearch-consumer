package org.elasticsearch.kafka.indexer.jobs;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.kafka.indexer.FailedEventsLogger;
import org.elasticsearch.kafka.indexer.exception.IndexerESNotRecoverableException;
import org.elasticsearch.kafka.indexer.exception.IndexerESRecoverableException;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.elasticsearch.kafka.indexer.service.OffsetLoggingCallbackImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author marinapopova Apr 13, 2016
 */
public class ConsumerWorker implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
	private IMessageHandler messageHandler;
	private final KafkaConsumer<String, String> consumer;
	private final String kafkaTopic;
	private final int consumerId;
	// interval in MS to poll Kafka brokers for messages, in case there were no
	// messages during the previous interval
	private long pollIntervalMs;
	private OffsetLoggingCallbackImpl offsetLoggingCallback;

	public ConsumerWorker(int consumerId, String consumerInstanceName, String kafkaTopic, Properties kafkaProperties,
			long pollIntervalMs, IMessageHandler messageHandler) {
		this.messageHandler = messageHandler;
		kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerInstanceName + "-" + consumerId);

		this.consumerId = consumerId;
		this.kafkaTopic = kafkaTopic;
		this.pollIntervalMs = pollIntervalMs;
		consumer = new KafkaConsumer<>(kafkaProperties);
		offsetLoggingCallback = new OffsetLoggingCallbackImpl();
		logger.info(
				"Created ConsumerWorker with properties: consumerId={}, consumerInstanceName={}, kafkaTopic={}, kafkaProperties={}",
				consumerId, consumerInstanceName, kafkaTopic, kafkaProperties);
	}

	@Override
	public void run() {
		try {
			logger.info("Starting ConsumerWorker, consumerId={}", consumerId);
			consumer.subscribe(Arrays.asList(kafkaTopic), offsetLoggingCallback);
			while (true) {
				boolean isPollFirstRecord = true;
				int numProcessedMessages = 0;
				int numSkippedIndexingMessages = 0;
				int numMessagesInBatch = 0;
				long pollStartMillis = 0L;

				logger.debug("consumerId={}; about to call consumer.poll() ...", consumerId);
				ConsumerRecords<String, String> records = consumer.poll(pollIntervalMs);
				Map<Integer, Long> partitionOffsetMap = new HashMap<>();

				// processing messages and adding them to ES batch
				for (ConsumerRecord<String, String> record : records) {
					numMessagesInBatch++;
					Map<String, Object> data = new HashMap<>();
					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("value", record.value());

					logger.debug("consumerId={}; received record: {}", consumerId, data);
					if (isPollFirstRecord) {
						isPollFirstRecord = false;
						logger.info("Start offset for partition {} in this poll : {}", record.partition(), record.offset());
						pollStartMillis = System.currentTimeMillis();
					}

					try {
						String processedMessage = messageHandler.transformMessage(record.value(), record.offset());
						messageHandler.addMessageToBatch(processedMessage);
						partitionOffsetMap.put(record.partition(), record.offset());
						numProcessedMessages++;
					} catch (Exception e) {
						numSkippedIndexingMessages++;

						logger.error("ERROR processing message {} - skipping it: {}", record.offset(), record.value(), e);
						FailedEventsLogger.logFailedToTransformEvent(record.offset(), e, record.value());
					}


				}
				long timeBeforePost = System.currentTimeMillis();

				// push to ES whole batch
				boolean moveToNextBatch = false;
				if (!records.isEmpty()) {				
					moveToNextBatch = postToElasticSearch();
					long timeToPost = System.currentTimeMillis() ;
					double perMessageTimeMillis = (double) (timeToPost - pollStartMillis) / numProcessedMessages;
                    logger.info("Previous poll snapshot: total-messages: {}, messages-processed: {}, messages-skipped: {}" +
                                    ", time-to-create-batch: {} ms, time-to-post-millis: {} ms, time-to-post-millis/messages-processed: {} ms"
                            , numMessagesInBatch
							, numProcessedMessages
							, numSkippedIndexingMessages
							, timeBeforePost-pollStartMillis
							, timeToPost-pollStartMillis
							, perMessageTimeMillis) ;
				}
				
				if (moveToNextBatch) {
					logger.info("Invoking commit for partition/offset : {}", partitionOffsetMap);
					consumer.commitAsync(offsetLoggingCallback);
				}

			}
		} catch (WakeupException e) {
			logger.warn("ConsumerWorker [consumerId={}] got WakeupException - exiting ... Exception: {}", consumerId, e);
			// ignore for shutdown
		} catch (IndexerESNotRecoverableException e){
			logger.error("ConsumerWorker [consumerId={}] got IndexerESNotRecoverableException - exiting ... Exception: {}", consumerId, e);
		} catch (Throwable e) {
			// TODO handle all kinds of Kafka-related exceptions here - to stop / re-init the consumer when needed
			logger.error("ConsumerWorker [consumerId={}] got Exception - exiting ... Exception: {}", consumerId, e);
		} finally {
			logger.warn("ConsumerWorker [consumerId={}] is shutting down ...", consumerId);
			consumer.close();
		}
	}
	
	private boolean postToElasticSearch() throws InterruptedException, IndexerESNotRecoverableException{
		boolean moveToTheNextBatch = true;
		try {
			messageHandler.postToElasticSearch();
		} catch (IndexerESRecoverableException e) {
			moveToTheNextBatch = false;
			logger.error("Error posting messages to Elastic Search - will re-try processing the batch; error: {}",
            e.getMessage());
		} 
		
		return moveToTheNextBatch;
	}

	public void shutdown() {
		logger.warn("ConsumerWorker [consumerId={}] shutdown() is called  - will call consumer.wakeup()", consumerId);
		consumer.wakeup();
	}

	public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
		return offsetLoggingCallback.getPartitionOffsetMap();
	}

	public int getConsumerId() {
		return consumerId;
	}
}
