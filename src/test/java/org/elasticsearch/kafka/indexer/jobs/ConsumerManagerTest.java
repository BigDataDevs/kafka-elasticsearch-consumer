package org.elasticsearch.kafka.indexer.jobs;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Vitalii Cherniak on 6/8/18.
 */
public class ConsumerManagerTest {
	private static final String TOPIC = "testTopic";
	private static final ConsumerManager CONSUMER_MANAGER = new ConsumerManager();
	private static final MockConsumer<String, String> CONSUMER = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
		@Override
		public synchronized void close() {
		}
	};
	private static final Set<TopicPartition> PARTITIONS = new HashSet<>();
	private static final Map<TopicPartition, Long> BEGINNING_OFFSETS = new HashMap<>();
	private static final Map<TopicPartition, Long> END_OFFSETS = new HashMap<>();
	private static final long BEGINNING_OFFSET_POSITION = 0L;
	private static final long END_OFFSET_POSITION = 100000L;

	@BeforeClass
	public static void setUp() {
		CONSUMER_MANAGER.setKafkaPollIntervalMs(100L);
		CONSUMER_MANAGER.setKafkaTopic(TOPIC);
		CONSUMER_MANAGER.setConsumerCustomStartOptionsFilePath(null);

		PARTITIONS.add(new TopicPartition(TOPIC, 0));
		PARTITIONS.add(new TopicPartition(TOPIC, 1));
		PARTITIONS.add(new TopicPartition(TOPIC, 2));
		PARTITIONS.add(new TopicPartition(TOPIC, 3));
		PARTITIONS.add(new TopicPartition(TOPIC, 4));
		CONSUMER.subscribe(Arrays.asList(TOPIC));
		PARTITIONS.forEach(topicPartition -> BEGINNING_OFFSETS.put(topicPartition, BEGINNING_OFFSET_POSITION));
		PARTITIONS.forEach(topicPartition -> END_OFFSETS.put(topicPartition, END_OFFSET_POSITION));
		CONSUMER.updateBeginningOffsets(BEGINNING_OFFSETS);
		CONSUMER.updateEndOffsets(END_OFFSETS);
	}

	@Test
	public void testRestartOffsets() {
		CONSUMER.rebalance(PARTITIONS);
		CONSUMER_MANAGER.determineOffsetForAllPartitionsAndSeek(StartOption.RESTART, CONSUMER);
		for (TopicPartition topicPartition: PARTITIONS) {
			Assert.assertEquals(BEGINNING_OFFSET_POSITION, CONSUMER.position(topicPartition));
		}
	}

	@Test
	public void testAllLatestOffsets() {
		CONSUMER.rebalance(PARTITIONS);
		CONSUMER_MANAGER.determineOffsetForAllPartitionsAndSeek(StartOption.LATEST, CONSUMER);
		for (TopicPartition topicPartition: PARTITIONS) {
			Assert.assertEquals(END_OFFSET_POSITION, CONSUMER.position(topicPartition));
		}
	}

	@Test
	public void testAllEarliestOffsets() {
		CONSUMER.rebalance(PARTITIONS);
		CONSUMER.seekToEnd(PARTITIONS);
		CONSUMER_MANAGER.determineOffsetForAllPartitionsAndSeek(StartOption.EARLIEST, CONSUMER);
		for (TopicPartition topicPartition: PARTITIONS) {
			Assert.assertEquals(BEGINNING_OFFSET_POSITION, CONSUMER.position(topicPartition));
		}
	}

	@Test
	public void testCustomOffsetsNoConfig() {
		CONSUMER.rebalance(PARTITIONS);
		CONSUMER_MANAGER.determineOffsetForAllPartitionsAndSeek(StartOption.CUSTOM, CONSUMER);
		for (TopicPartition topicPartition: PARTITIONS) {
			Assert.assertEquals(BEGINNING_OFFSET_POSITION, CONSUMER.position(topicPartition));
		}
	}

	@Test
	public void testCustomOffsetsFromFileNotEnoughPartitions() {
		//Test custom start options and with not enough partitions defined, so 'RESTART' option will be used for all partitions
		CONSUMER_MANAGER.setConsumerCustomStartOptionsFilePath("src/test/resources/test-start-options-custom.properties");
		CONSUMER.rebalance(PARTITIONS);
		CONSUMER_MANAGER.determineOffsetForAllPartitionsAndSeek(StartOption.CUSTOM, CONSUMER);
		for (TopicPartition topicPartition: PARTITIONS) {
			Assert.assertEquals(BEGINNING_OFFSET_POSITION, CONSUMER.position(topicPartition));
		}
	}

	@Test
	public void testCustomOffsetsFromFile() {
		Map<Integer, Long> expectedOffsets = StartOptionParser.getCustomStartOffsets("src/test/resources/test-start-options-custom-5-partitions.properties");
		CONSUMER_MANAGER.setConsumerCustomStartOptionsFilePath("src/test/resources/test-start-options-custom-5-partitions.properties");
		CONSUMER.rebalance(PARTITIONS);
		CONSUMER_MANAGER.determineOffsetForAllPartitionsAndSeek(StartOption.CUSTOM, CONSUMER);
		for (TopicPartition topicPartition: PARTITIONS) {
			Assert.assertEquals(expectedOffsets.get(topicPartition.partition()).longValue(), CONSUMER.position(topicPartition));
		}
	}
}
