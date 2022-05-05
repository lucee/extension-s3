package org.lucee.extension.resource.s3.pool;

import com.amazonaws.services.s3.AmazonS3;

public interface AmazonS3PoolListener {

	public void doAddObject(AmazonS3Pool pool) throws AmazonS3PoolListenerException;

	public void doBorrowObject(AmazonS3Pool pool, long borrowMaxWaitMillis) throws AmazonS3PoolListenerException;

	public void doClear(AmazonS3Pool pool);

	public void doClose(AmazonS3Pool pool);

	public void doEvict(AmazonS3Pool pool) throws AmazonS3PoolListenerException;

	public void returnObject(AmazonS3Pool pool, AmazonS3 as3);
}
