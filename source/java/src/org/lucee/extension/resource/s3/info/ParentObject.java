package org.lucee.extension.resource.s3.info;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Exception;

import com.amazonaws.services.s3.model.Owner;

import lucee.commons.io.log.Log;
import lucee.runtime.type.Array;
import lucee.runtime.type.Struct;

public class ParentObject extends S3InfoSupport {

	// private S3Info kid;
	private String bucketName;
	private String objectName;
	private long validUntil;

	public ParentObject(S3 s3, String objectName, S3Info info, Log log) {
		super(s3, log);
		this.bucketName = info.getBucketName();
		this.objectName = objectName;
		this.validUntil = info.validUntil();
	}

	public ParentObject(S3 s3, String bucketName, String objectName, long validUntil, Log log) {
		super(s3, log);
		this.bucketName = bucketName;
		this.objectName = objectName;
		this.validUntil = validUntil;
	}

	@Override
	public long getSize() {
		return 0;
	}

	@Override
	public String getName() {
		return getObjectName();
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
	public long getLastModified() {
		return 0;
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

	@Override
	public Struct getMetaData() throws S3Exception {
		Struct data = super.getMetaData();
		Array acl = s3.getAccessControlList(getBucketName(), null);
		if (acl != null) data.put("acl", acl);
		return data;
	}

	@Override
	public String toString() {
		return "bucket:" + getBucketName() + ";key:" + getObjectName() + ";";
	}

}
