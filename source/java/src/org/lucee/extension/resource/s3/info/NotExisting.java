package org.lucee.extension.resource.s3.info;

import java.util.HashMap;
import java.util.Map;

import org.jets3t.service.model.StorageOwner;

public class NotExisting extends S3InfoSupport {

	private String bucketName;
	private String objectName;
	private String location;
	private long validUntil;

	public NotExisting(String bucketName, String objectName, String location, long validUntil) {
		this.bucketName=bucketName;
		this.objectName=objectName;
		this.location=location;
		this.validUntil=validUntil;
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
	public StorageOwner getOwner() {
		return null;
	}

	@Override
	public String getLocation() {
		return null;
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
