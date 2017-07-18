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

/**
 * Created by Vitalii Cherniak on 04.10.16.
 */
public class ConsumerStartOption {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerStartOption.class);
	public static final int DEFAULT = -1;

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

		String[] values = property.split(":");
		if (values.length < 2) {
			throw new IllegalArgumentException("Wrong consumer start option format. Cannot split '" + property + "'");
		}
		if (values[0].equalsIgnoreCase("default")) {
			partition = DEFAULT; //mark as default option
		} else {
			partition = Integer.valueOf(values[0]);
		}
		startFrom = StartFrom.valueOf(values[1]);
		startOffset = 0L;
		if (startFrom == StartFrom.CUSTOM) {
			if (values.length == 3) {
				startOffset = Long.valueOf(values[2]);
			} else {
				throw new IllegalArgumentException("Cannot parse CUSTOM start offset in consumer start option '" + property + "'");
			}
		}
	}

	public static Map<Integer, ConsumerStartOption> fromFile(String configFilePath) throws IllegalArgumentException {
		Map<Integer, ConsumerStartOption> config = new HashMap<>();
		if (!StringUtils.isEmpty(configFilePath)) {
			File configFile = new File(configFilePath);
		if (configFile.exists()) {
			try {
				List<String> lines = Files.readAllLines(configFile.toPath());
				lines.stream()
						//filter empty lines and comments (lines starts with '#')
						.filter(line -> !line.isEmpty() && !line.startsWith("#"))
						.forEach(line -> {
							ConsumerStartOption option = new ConsumerStartOption(line);
							config.put(option.getPartition(), option);
						});
			} catch (IOException e) {
				String message = "Unable to read Consumer start options configuration file from '" +
						configFile.getPath() + "'";
				logger.error(message);
				throw new IllegalArgumentException(message);
			}
		} else {
			logger.warn("Consumer start options configuration file '"
					+ configFile.getPath() + "' doesn't exist. Consumer will use 'RESTART' option by default");
		}
		} else {
			logger.info("Consumer start options configuration file is not defined. Consumer will use 'RESTART' option by default");
		}

		//check for default option
		if (!config.containsKey(DEFAULT)) {
			config.put(DEFAULT, new ConsumerStartOption(DEFAULT, StartFrom.RESTART, 0L));
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
}