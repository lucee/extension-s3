package org.lucee.extension.resource.s3;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;

/**
 * Result of deleting several objects with one call: the objects that are gone (an object that did
 * not exist counts as deleted, like with the DeleteObjects operation of S3) and the objects that
 * could not be deleted.
 */
public final class S3DeleteResult {

	private final List<KeyVersion> deleted = new ArrayList<>();
	private final List<Failure> errors = new ArrayList<>();

	/**
	 * @return the keys (and version ids if a version was deleted) that are deleted, in the order they
	 *         were passed
	 */
	public List<KeyVersion> getDeleted() {
		return deleted;
	}

	/**
	 * @return the objects that could not be deleted
	 */
	public List<Failure> getErrors() {
		return errors;
	}

	public static final class Failure {
		private final String key;
		private final String versionId;
		private final String code;
		private final String message;

		public Failure(String key, String versionId, String code, String message) {
			this.key = key;
			this.versionId = versionId;
			this.code = code;
			this.message = message;
		}

		public String getKey() {
			return key;
		}

		/**
		 * @return the version id of the failed delete or null if no version was addressed
		 */
		public String getVersionId() {
			return versionId;
		}

		public String getCode() {
			return code;
		}

		public String getMessage() {
			return message;
		}
	}
}
