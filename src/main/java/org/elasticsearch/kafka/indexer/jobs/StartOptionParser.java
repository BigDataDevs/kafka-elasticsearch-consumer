package org.elasticsearch.kafka.indexer.jobs;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Created by Vitalii Cherniak on 04.10.16.
 */
public class StartOptionParser {
	private static final Logger logger = LoggerFactory.getLogger(StartOptionParser.class);
	private static final String RESTART_BY_DEFAULT_MSG = "Consumer will use 'RESTART' option by default";

	/**
	 * Creates StartOption from {@link String} value
	 * @param startOptionStr start option: RESTART, EARLIEST, LATEST or CUSTOM
	 * @return StartOption
	 * @throws IllegalArgumentException in case of wrong option value
	 */
	public static StartOption getStartOption(String startOptionStr) throws IllegalArgumentException {
		if (StringUtils.isEmpty(startOptionStr)) {
			logger.info("Consumer start option not defined. " + RESTART_BY_DEFAULT_MSG);
			return StartOption.RESTART;
		}

		StartOption startOption;
		try {
			startOption = StartOption.valueOf(startOptionStr.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Wrong consumer start option. Option = '" + startOptionStr + "'", e);
		}

		return startOption;
	}

	/**
	 * Gets custom start offsets per partition from configuration file
	 * @param customStartOptionsFileStr absolute path to custom start options config file
	 * @return map of partition (key) to start offset (value) or empty map in case of malformed config
	 * @throws IllegalArgumentException in case of error with file reading
	 */
	public static Map<Integer, Long> getCustomStartOffsets(String customStartOptionsFileStr) throws IllegalArgumentException {
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
		Map<Integer, Long> customStartOffsets = new HashMap<>();
		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(configFile));
			customStartOffsets = properties.entrySet()
					.stream()
					.collect(Collectors.toMap(entry -> Integer.valueOf((String)entry.getKey()), entry -> Long.valueOf((String)entry.getValue())));
		} catch (NumberFormatException e) {
			logger.warn("Wrong consumer custom start option in file '{}'. " + RESTART_BY_DEFAULT_MSG, customStartOptionsFileStr, e);
			customStartOffsets.clear();
		} catch (IOException e) {
			String message = "Unable to read Consumer start options configuration file from '" + configFile.getPath() + "'";
			logger.error(message);
			throw new IllegalArgumentException(message);
		}

		return customStartOffsets;
	}

	public enum StartOption {
		CUSTOM,
		EARLIEST,
		LATEST,
		RESTART
	}
}