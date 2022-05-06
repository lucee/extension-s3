package org.lucee.extension.resource.s3.pool;

import java.io.IOException;

public class AmazonS3PoolListenerException extends IOException {

	private static final long serialVersionUID = 1979830958008436318L;

	public AmazonS3PoolListenerException(String message, Throwable cause) {
		super(message, cause);
	}

	public AmazonS3PoolListenerException(String message) {
		super(message);
	}

}
