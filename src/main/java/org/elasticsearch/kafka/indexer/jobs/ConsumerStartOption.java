package org.elasticsearch.kafka.indexer.jobs;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Vitalii Cherniak on 04.10.16.
 */
public class ConsumerStartOption {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerStartOption.class);
	public static final int ALL_PARTITIONS = -1;
	public static final ConsumerStartOption RESTART_OPTION = new ConsumerStartOption(ALL_PARTITIONS, StartFrom.RESTART, 0L);
	private static final Pattern OPTION_REGEX = Pattern.compile("^(RESTART$|LATEST$|EARLIEST$)|(CUSTOM\\:(\\d+)$)|((\\d+)=(\\d+)$)", Pattern.CASE_INSENSITIVE);

	private int partition;
	private StartFrom startFrom;
	private long startOffset;

	public ConsumerStartOption(int partition, StartFrom startFrom, long startOffset) {
		this.partition = partition;
		this.startFrom = startFrom;
		this.startOffset = startOffset;
	}

	public ConsumerStartOption(String property) throws IllegalArgumentException {
		if (property == null) {
			throw new IllegalArgumentException("Option value cannot be null");
		}

		Matcher matcher = OPTION_REGEX.matcher(property);
		if (!matcher.find()) {
			throw new IllegalArgumentException("Wrong consumer start option format. Option = '" + property + "'");
		}

		startOffset = 0L;
		partition = ALL_PARTITIONS;
		// RESTART, LATEST, EARLIEST
		if (matcher.group(1) != null) {
			startFrom = StartFrom.valueOf(matcher.group(1).toUpperCase());
			return;
		}

		// CUSTOM:offset
		if (matcher.group(2) != null) {
			startFrom = StartFrom.CUSTOM;
			startOffset = Long.valueOf(matcher.group(3));
			return;
		}

		// partition:CUSTOM:offset
		if (matcher.group(4) != null) {
			startFrom = StartFrom.CUSTOM;
			partition = Integer.valueOf(matcher.group(5));
			startOffset = Long.valueOf(matcher.group(6));
		}
	}

	public static Map<Integer, ConsumerStartOption> fromConfig(String configStr) throws IllegalArgumentException {
		final Map<Integer, ConsumerStartOption> config = new HashMap<>();
		// set 'RESTART' option by default
		config.put(ALL_PARTITIONS, RESTART_OPTION);

		if (StringUtils.isEmpty(configStr)) {
			logger.info("Consumer start option or start configuration file is not defined. Consumer will use 'RESTART' option by default");
			return config;
		}

		File configFile = new File(configStr);
		ConsumerStartOption startFromOption = null;
		try {
			startFromOption = new ConsumerStartOption(configStr);
		} catch (IllegalArgumentException e) {
			if (!configFile.exists()) {
				logger.warn("Wrong consumer start option '{}'. Consumer will use 'RESTART' option by default", configStr, e);
				return config;
			}
		}

		if (startFromOption != null) {
			config.put(ALL_PARTITIONS, startFromOption);
			return config;
		}

		if (!configFile.exists()) {
			logger.warn("Consumer start options configuration file {} doesn't exist." +
					"Consumer will use 'RESTART' option by default", configFile.getPath());
			return config;
		}

		config.clear();
		//read custom option from file
		try {
			List<String> lines = Files.readAllLines(configFile.toPath());
			lines.stream()
					//filter empty lines and comments (lines starts with '#')
					.filter(line -> !line.isEmpty() && !line.startsWith("#"))
					.forEach(line -> {
						ConsumerStartOption option = new ConsumerStartOption(line);
						config.put(option.getPartition(), option);
					});
		} catch (IllegalArgumentException e) {
			logger.warn("Wrong consumer custom start option in file '{}'. Consumer will use 'RESTART' option by default", configStr, e);
			config.clear();
		} catch (IOException e) {
			String message = "Unable to read Consumer start options configuration file from '" + configFile.getPath() + "'";
			logger.error(message);
			throw new IllegalArgumentException(message);
		}

		// use 'RESTART' option if custom config is empty
		if (config.isEmpty()) {
			logger.warn("Consumer custom start config is empty. Consumer will use 'RESTART' option by default");
			config.put(ALL_PARTITIONS, RESTART_OPTION);
		}

		return config;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public StartFrom getStartFrom() {
		return startFrom;
	}

	public void setStartFrom(StartFrom startFrom) {
		this.startFrom = startFrom;
	}

	public long getStartOffset() {
		return startOffset;
	}

	public void setStartOffset(long startOffset) {
		this.startOffset = startOffset;
	}

	public enum StartFrom {
		CUSTOM,
		EARLIEST,
		LATEST,
		RESTART
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof ConsumerStartOption)) return false;
		ConsumerStartOption that = (ConsumerStartOption) o;
		return partition == that.partition &&
				startOffset == that.startOffset &&
				startFrom == that.startFrom;
	}

	@Override
	public int hashCode() {
		return Objects.hash(partition, startFrom, startOffset);
	}
}