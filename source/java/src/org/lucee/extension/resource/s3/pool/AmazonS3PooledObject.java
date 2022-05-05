package org.lucee.extension.resource.s3.pool;

import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.amazonaws.services.s3.AmazonS3;

public class AmazonS3PooledObject extends DefaultPooledObject<AmazonS3> {

	public final long created;
	public long lastAccess;

	public AmazonS3PooledObject(AmazonS3 amazons3) {
		super(amazons3);
		this.lastAccess = this.created = System.currentTimeMillis();
	}
}
