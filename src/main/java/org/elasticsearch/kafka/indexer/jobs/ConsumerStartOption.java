package org.elasticsearch.kafka.indexer.jobs;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
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
	public static final ConsumerStartOption RESTART_OPTION = new ConsumerStartOption(StartFrom.RESTART);
	private static final Pattern CUSTOM_OPTION_REGEX = Pattern.compile("^(\\d+)=(\\d+)$");
	private static final String RESTART_BY_DEFAULT_MSG = "Consumer will use 'RESTART' option by default";

	private int partition;
	private StartFrom startFrom;
	private long startOffset;

	/**
	 * Constructor for start option
	 * @param partition number of partition
	 * @param startFrom start option
	 * @param startOffset start offset
	 */
	public ConsumerStartOption(int partition, StartFrom startFrom, long startOffset) {
		this.partition = partition;
		this.startFrom = startFrom;
		this.startOffset = startOffset;
	}

	/**
	 * Constructor for custom start option in next format: partition=start_offset
	 * @param customOptionStr
	 * @throws IllegalArgumentException
	 */
	public ConsumerStartOption(String customOptionStr) throws IllegalArgumentException {
		if (customOptionStr == null) {
			throw new IllegalArgumentException("Option value cannot be null");
		}

		Matcher matcher = CUSTOM_OPTION_REGEX.matcher(customOptionStr.trim());
		if (!matcher.find()) {
			throw new IllegalArgumentException("Wrong consumer custom start option format. Option = '" + customOptionStr + "'");
		}

		startFrom = StartFrom.CUSTOM;
		partition = Integer.valueOf(matcher.group(1));
		startOffset = Long.valueOf(matcher.group(2));
	}

	/**
	 * Constructor for start option that applied for all partitions
	 * @param startFrom start option
	 */
	public ConsumerStartOption(StartFrom startFrom) {
		this(ALL_PARTITIONS, startFrom, 0L);
	}

	/**
	 * Creates a map of start options. The key is a partition number or -1 that means applying for all partitions. The value is a start option.
	 * @param startOptionStr start option: RESTART, EARLIEST, LATEST or CUSTOM
	 * @param customStartOptionsFileStr absolute path to custom start options config file, used when @param startOptionStr is CUSTOM
	 * @return map of start options or empty map in case of parsing error
	 * @throws IllegalArgumentException in case of wrong option value or problem with custom config file reading
	 */
	public static Map<Integer, ConsumerStartOption> fromConfig(String startOptionStr, String customStartOptionsFileStr) throws IllegalArgumentException {
		final Map<Integer, ConsumerStartOption> config = new HashMap<>();

		if (StringUtils.isEmpty(startOptionStr)) {
			logger.info("Consumer start option or start configuration file is not defined. " + RESTART_BY_DEFAULT_MSG);
			return Collections.emptyMap();
		}

		StartFrom startOption;
		try {
			startOption = StartFrom.valueOf(startOptionStr.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Wrong consumer start option. Option = '" + startOptionStr + "'", e);
		}

		if (startOption == StartFrom.CUSTOM) {
			if (StringUtils.isEmpty(customStartOptionsFileStr)) {
				logger.warn("Consumer custom start options configuration file is not specified. " + RESTART_BY_DEFAULT_MSG);
				return Collections.emptyMap();
			}
 			File configFile = new File(customStartOptionsFileStr);
			if (!configFile.exists()) {
				logger.warn("Consumer custom start options configuration file {} doesn't exist." +
						RESTART_BY_DEFAULT_MSG, configFile.getPath());
				return Collections.emptyMap();
			}

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
				logger.warn("Wrong consumer custom start option in file '{}'. " + RESTART_BY_DEFAULT_MSG, startOptionStr, e);
				config.clear();
			} catch (IOException e) {
				String message = "Unable to read Consumer start options configuration file from '" + configFile.getPath() + "'";
				logger.error(message);
				throw new IllegalArgumentException(message);
			}
		} else {
			config.put(ALL_PARTITIONS, new ConsumerStartOption(startOption));
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