package org.elasticsearch.kafka.indexer.jobs;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.kafka.indexer.jobs.ConsumerStartOption.StartFrom;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author marinapopova
 * Apr 2, 2018
 */
public class ConsumerStartOptionTest {

	@Test
	public void testRestartOption() {
		Map<Integer, ConsumerStartOption> configMap = ConsumerStartOption.fromConfig("RESTART");
		Assert.assertNotNull(configMap);
		Assert.assertEquals(configMap.size(), 1);
		Assert.assertEquals(configMap.get(ConsumerStartOption.ALL_PARTITIONS), ConsumerStartOption.RESTART_OPTION);
	}

	@Test
	public void testEarliestOption() {
		Map<Integer, ConsumerStartOption> expectedMap = new HashMap<>();
		expectedMap.put(ConsumerStartOption.ALL_PARTITIONS,
				new ConsumerStartOption(ConsumerStartOption.ALL_PARTITIONS, StartFrom.EARLIEST, 0L));
		Map<Integer, ConsumerStartOption> resultMap = ConsumerStartOption.fromConfig("EARLIEST");
		Assert.assertNotNull(resultMap);
		Assert.assertEquals(expectedMap, resultMap);
	}

	@Test
	public void testLatestOption() {
		Map<Integer, ConsumerStartOption> expectedMap = new HashMap<>();
		expectedMap.put(ConsumerStartOption.ALL_PARTITIONS,
				new ConsumerStartOption(ConsumerStartOption.ALL_PARTITIONS, StartFrom.LATEST, 0L));
		Map<Integer, ConsumerStartOption> resultMap = ConsumerStartOption.fromConfig("LATEST");
		Assert.assertNotNull(resultMap);
		Assert.assertEquals(expectedMap, resultMap);
	}

	@Test
	public void testCustomOption() {
		Map<Integer, ConsumerStartOption> expectedMap = new HashMap<>();
		expectedMap.put(ConsumerStartOption.ALL_PARTITIONS,
				new ConsumerStartOption(ConsumerStartOption.ALL_PARTITIONS, StartFrom.CUSTOM, 222L));
		Map<Integer, ConsumerStartOption> resultMap = ConsumerStartOption.fromConfig("CUSTOM:222");
		Assert.assertNotNull(resultMap);
		Assert.assertEquals(expectedMap, resultMap);
	}

	@Test
	public void testWrongOption() {
		Map<Integer, ConsumerStartOption> configMap = ConsumerStartOption.fromConfig("sferggbgg");
		Assert.assertNotNull(configMap);
		Assert.assertEquals(configMap.size(), 1);
		Assert.assertEquals(configMap.get(ConsumerStartOption.ALL_PARTITIONS), ConsumerStartOption.RESTART_OPTION);
	}

	@Test
	public void testCustomOptionsFromFile() {
		Map<Integer, ConsumerStartOption> expectedMap = new HashMap<>();
		expectedMap.put(0, new ConsumerStartOption(0, StartFrom.CUSTOM, 10L));
		expectedMap.put(1, new ConsumerStartOption(1, StartFrom.CUSTOM, 20L));
		Map<Integer, ConsumerStartOption> resultMap = ConsumerStartOption.fromConfig(
				"src/test/resources/test-start-options-custom.config");
		Assert.assertNotNull(resultMap);
		Assert.assertEquals(expectedMap, resultMap);
	}

	@Test
	public void testCustomOptionsFromMalformedFile() {
		Map<Integer, ConsumerStartOption> configMap = ConsumerStartOption.fromConfig(
				"src/test/resources/test-start-options-custom-malformed.config");
		Assert.assertNotNull(configMap);
		Assert.assertEquals(configMap.size(), 1);
		Assert.assertEquals(configMap.get(ConsumerStartOption.ALL_PARTITIONS), ConsumerStartOption.RESTART_OPTION);
	}

	@Test
	public void testCustomOptionsFromEmptyFile() {
		Map<Integer, ConsumerStartOption> configMap = ConsumerStartOption.fromConfig(
				"src/test/resources/test-start-options-custom-empty.config");
		Assert.assertNotNull(configMap);
		Assert.assertEquals(configMap.size(), 1);
		Assert.assertEquals(configMap.get(ConsumerStartOption.ALL_PARTITIONS), ConsumerStartOption.RESTART_OPTION);
	}

	@Test
	public void testEmptyOption() {
		Map<Integer, ConsumerStartOption> configMap = ConsumerStartOption.fromConfig("");
		Assert.assertNotNull(configMap);
		Assert.assertEquals(configMap.size(), 1);
		Assert.assertEquals(configMap.get(ConsumerStartOption.ALL_PARTITIONS), ConsumerStartOption.RESTART_OPTION);
	}
}
