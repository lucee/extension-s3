package org.lucee.extension.resource.s3.info;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Exception;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Owner;

import lucee.commons.io.log.Log;
import lucee.runtime.type.Array;
import lucee.runtime.type.Struct;

public class S3BucketWrapper extends S3InfoSupport implements S3BucketInfo {

	private Bucket bucket;
	private long validUntil;

	public S3BucketWrapper(S3 s3, Bucket bucket, long validUntil, Log log) {
		super(s3, log);
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

	@Override
	public Struct getMetaData() throws S3Exception {
		Struct data = super.getMetaData();

		Array acl = s3.getAccessControlList(getBucketName(), null);
		if (acl != null) data.setEL("acl", acl);

		data.setEL("creationDate", bucket.getCreationDate());
		return data;
	}

}
