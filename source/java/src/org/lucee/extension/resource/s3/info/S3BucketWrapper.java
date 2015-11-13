package org.lucee.extension.resource.s3.info;

import org.jets3t.service.model.S3Bucket;

public class S3BucketWrapper implements S3Info {

	private S3Bucket bucket;

	public S3BucketWrapper(S3Bucket bucket) {
		this.bucket=bucket;
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
}
