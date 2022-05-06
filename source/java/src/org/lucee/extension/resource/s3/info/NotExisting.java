package org.lucee.extension.resource.s3.info;

import com.amazonaws.services.s3.model.Owner;

import lucee.commons.io.log.Log;

public class NotExisting extends S3InfoSupport {

	private String bucketName;
	private String objectName;
	private long validUntil;

	public NotExisting(String bucketName, String objectName, long validUntil, Log log) {
		super(null, log);
		this.bucketName = bucketName;
		this.objectName = objectName;
		this.validUntil = validUntil;
	}

	@Override
	public String getName() {
		return objectName;
	}

	@Override
	public String getObjectName() {
		return objectName;
	}

	@Override
	public String getBucketName() {
		return bucketName;
	}

	@Override
	public long getSize() {
		return 0;
	}

	@Override
	public long getLastModified() {
		return 0;
	}

	@Override
	public boolean exists() {
		return false;
	}

	@Override
	public boolean isDirectory() {
		return false;
	}

	@Override
	public boolean isFile() {
		return false;
	}

	@Override
	public boolean isBucket() {
		return false;
	}

	@Override
	public long validUntil() {
		return validUntil;
	}

	@Override
	public Owner getOwner() {
		return null;
	}

	@Override
	public boolean isVirtual() {
		return true;
	}

}
