package org.lucee.extension.resource.s3;

/**
 * Thrown when a conditional request (ifMatch, ifNoneMatch, ifModifiedSince, ifUnmodifiedSince) was
 * rejected by S3 because the condition was not met. This is a regular, expected outcome of a
 * conditional request and is reported as a distinct error, so it can be told apart from any other
 * S3 failure.
 */
public class S3PreconditionFailedException extends S3Exception {

	private static final long serialVersionUID = 6840217563322310467L;

	public static final String ERROR_CODE = "PreconditionFailed";

	/**
	 * the type of the CFML exception
	 */
	public static final String TYPE = "s3.preconditionfailed";

	public S3PreconditionFailedException(String message) {
		super(message);
		setErrorCode(ERROR_CODE);
	}

	public S3PreconditionFailedException(String message, Throwable t) {
		super(message, t);
		setErrorCode(ERROR_CODE);
	}
}
