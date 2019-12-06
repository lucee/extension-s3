package org.lucee.extension.resource.s3.info;

public abstract class S3InfoSupport implements S3Info {

	public boolean equals(Object obj) {
		if (!(obj instanceof S3Info)) return false;
		S3Info other = (S3Info) obj;
		if (getBucketName() == null) {
			if (other.getBucketName() != null) return false;
		}
		else if (!getBucketName().equals(other.getBucketName())) return false;

		if (getObjectName() == null) {
			if (other.getObjectName() != null) return false;
		}
		return getObjectName().equals(other.getObjectName());
	}
}
