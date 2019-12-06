package org.lucee.extension.resource.s3.info;

import java.util.Map;

import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.StorageOwner;

public class S3BucketWrapper extends S3InfoSupport {

	private S3Bucket bucket;
	private long validUntil;

	public S3BucketWrapper(S3Bucket bucket, long validUntil) {
		this.bucket = bucket;
		this.validUntil = validUntil;
	}

	@Override
	public long getSize() {
		return 0;
	}

	@Override
	public long getLastModified() {
		return bucket.getCreationDate().getTime();
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
		return bucket.getName();
	}

	@Override
	public boolean isBucket() {
		return true;
	}

	public String getLocation() {
		return bucket.getLocation();
	}

	@Override
	public long validUntil() {
		return validUntil;
	}

	public S3Bucket getBucket() {
		return bucket;
	}

	@Override
	public String getName() {
		return getBucketName();
	}

	@Override
	public StorageOwner getOwner() {
		return bucket.getOwner();
	}

	@Override
	public Map<String, Object> getMetaData() {
		return bucket.getMetadataMap();
	}

	@Override
	public boolean isVirtual() {
		return false;
	}
}
