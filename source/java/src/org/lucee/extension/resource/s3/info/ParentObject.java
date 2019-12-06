package org.lucee.extension.resource.s3.info;

import java.util.HashMap;
import java.util.Map;

import org.jets3t.service.model.StorageOwner;

public class ParentObject extends S3InfoSupport {

	// private S3Info kid;
	private String bucketName;
	private String objectName;
	private String location;
	private long validUntil;

	public ParentObject(String objectName, S3Info info) {
		this.bucketName = info.getBucketName();
		this.objectName = objectName;
		this.location = info.getLocation();
		this.validUntil = info.validUntil();
	}

	public ParentObject(String bucketName, String objectName, String location, long validUntil) {
		this.bucketName = bucketName;
		this.objectName = objectName;
		this.location = location;
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
	public StorageOwner getOwner() {
		return null;
	}

	@Override
	public String getLocation() {
		return location;
	}

	@Override
	public Map<String, Object> getMetaData() {
		return new HashMap<String, Object>();
	}

	@Override
	public boolean isVirtual() {
		return true;
	}

}
