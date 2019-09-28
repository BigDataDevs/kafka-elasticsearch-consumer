/**
  * @author marinapopova
  * Sep 25, 2015
 */
package org.elasticsearch.kafka.indexer.exception;

public class ConsumerNonRecoverableException extends Exception {

	/**
	 * 
	 */
	public ConsumerNonRecoverableException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public ConsumerNonRecoverableException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public ConsumerNonRecoverableException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ConsumerNonRecoverableException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public ConsumerNonRecoverableException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
