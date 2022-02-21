package org.lucee.extension.resource.s3.info;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Owner;

public class S3BucketWrapper extends S3InfoSupport {

	private Bucket bucket;
	private long validUntil;

	public S3BucketWrapper(Bucket bucket, long validUntil) {
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

	@Override
	public long validUntil() {
		return validUntil;
	}

	public Bucket getBucket() {
		return bucket;
	}

	@Override
	public String getName() {
		return getBucketName();
	}

	@Override
	public Owner getOwner() {
		return bucket.getOwner();
	}

	@Override
	public boolean isVirtual() {
		return false;
	}

	@Override
	public String toString() {

		return bucket + ":" + validUntil;
	}
}
