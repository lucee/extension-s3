package org.lucee.extension.resource.s3.info;

import org.lucee.extension.resource.s3.S3;

import com.amazonaws.services.s3.model.Owner;

import lucee.commons.io.log.Log;

public class ParentAndStorageObject extends S3InfoSupport {

	private ParentObject po;
	private StorageObjectWrapper sow;

	public ParentAndStorageObject(S3 s3, ParentObject po, StorageObjectWrapper sow, Log log) {
		super(s3, log);
		this.po = po;
		this.sow = sow;
	}

	@Override
	public String getName() {
		return sow.getName();
	}

	@Override
	public String getObjectName() {
		return sow.getObjectName();
	}

	@Override
	public String getBucketName() {
		return sow.getBucketName();
	}

	@Override
	public long getSize() {
		return sow.getSize();
	}

	@Override
	public long getLastModified() {
		return sow.getLastModified();
	}

	@Override
	public boolean exists() {
		return sow.exists() || po.exists(); // could also simply be true
	}

	@Override
	public boolean isDirectory() {
		return sow.isDirectory() || po.isDirectory(); // could also simply be true
	}

	@Override
	public boolean isFile() {
		return sow.isFile() || po.isFile(); // could also simply be true
	}

	@Override
	public boolean isBucket() {
		return sow.isBucket() || po.isBucket(); // could also simply be true
	}

	@Override
	public long validUntil() {
		return sow.validUntil();
	}

	@Override
	public Owner getOwner() {
		return sow.getOwner();
	}

	@Override
	public boolean isVirtual() {
		return sow.isVirtual() || po.isVirtual(); // could also simply be true
	}

}
