package org.lucee.extension.resource.s3.info;

import java.util.Map;

import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.StorageOwner;

public class AccessDeniedBukcet extends S3InfoSupport implements S3BucketInfo {

	private String bucketName;
	private long validUntil;
	private Exception exp;

	public AccessDeniedBukcet(String bucketName, long validUntil, Exception exp) {
		this.bucketName = bucketName;
		this.validUntil = validUntil;
		this.exp = exp;
	}

	@Override
	public long getSize() {
		return 0;
	}

	@Override
	public long getLastModified() {
		throw new RuntimeException(exp);
	}

	@Override
	public boolean exists() {
		return true;
	}

	@Override
	public boolean isDirectory() {
		return true;
	}

	@Override
	public boolean isFile() {
		return false;
	}

	@Override
	public String getObjectName() {
		return null;
	}

	@Override
	public String getBucketName() {
		return bucketName;
	}

	@Override
	public boolean isBucket() {
		return true;
	}

	@Override
	public String getLocation() {
		return null;
	}

	@Override
	public long validUntil() {
		return validUntil;
	}

	@Override
	public S3Bucket getBucket() {
		throw new RuntimeException(exp);
	}

	@Override
	public String getName() {
		return getBucketName();
	}

	@Override
	public StorageOwner getOwner() {
		throw new RuntimeException(exp);
	}

	@Override
	public Map<String, Object> getMetaData() {
		throw new RuntimeException(exp);
	}

	@Override
	public boolean isVirtual() {
		return false;
	}

	@Override
	public String toString() {
		return bucketName + ":" + validUntil;
	}

}
