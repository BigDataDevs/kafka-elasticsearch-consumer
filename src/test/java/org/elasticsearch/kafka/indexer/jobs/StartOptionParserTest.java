package org.elasticsearch.kafka.indexer.jobs;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.kafka.indexer.jobs.StartOptionParser.StartOption;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author marinapopova
 * Apr 2, 2018
 */
public class StartOptionParserTest {

	@Test
	public void testRestartOption() {
		StartOptionParser.StartOption startOption = StartOptionParser.getStartOption("RESTART");
		Assert.assertEquals(StartOption.RESTART, startOption);
	}

	@Test
	public void testEarliestOption() {
		StartOptionParser.StartOption startOption = StartOptionParser.getStartOption("EARLIEST");
		Assert.assertEquals(StartOption.EARLIEST, startOption);
	}

	@Test
	public void testLatestOption() {
		StartOptionParser.StartOption startOption = StartOptionParser.getStartOption("LATEST");
		Assert.assertEquals(StartOption.LATEST, startOption);
	}

	@Test
	public void testCustomOption() {
		StartOptionParser.StartOption startOption = StartOptionParser.getStartOption("CUSTOM");
		Assert.assertEquals(StartOption.CUSTOM, startOption);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWrongOption() {
		StartOptionParser.StartOption startOption = StartOptionParser.getStartOption("sferggbgg");
	}

	@Test
	public void testEmptyOption() {
		StartOptionParser.StartOption startOption = StartOptionParser.getStartOption("");
		Assert.assertEquals(StartOption.RESTART, startOption);
	}

	@Test
	public void testCustomOptionsFromFile() {
		Map<Integer, Long> expectedMap = new HashMap<>();
		expectedMap.put(0, 10L);
		expectedMap.put(1, 20L);
		Map<Integer, Long> resultMap = StartOptionParser.getCustomStartOffsets("src/test/resources/test-start-options-custom.properties");
		Assert.assertNotNull(resultMap);
		Assert.assertEquals(expectedMap, resultMap);
	}

	@Test
	public void testCustomOptionsFromMalformedFile() {
		Map<Integer, Long> configMap = StartOptionParser.getCustomStartOffsets("src/test/resources/test-start-options-custom-malformed.properties");
		Assert.assertNotNull(configMap);
		Assert.assertEquals(configMap.size(), 0);
	}

	@Test
	public void testCustomOptionsFromEmptyFile() {
		Map<Integer, Long> configMap = StartOptionParser.getCustomStartOffsets("src/test/resources/test-start-options-custom-empty.properties");
		Assert.assertNotNull(configMap);
		Assert.assertEquals(configMap.size(), 0);
	}
}
