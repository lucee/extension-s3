package org.lucee.extension.resource.s3.pool;

import com.amazonaws.services.s3.AmazonS3;

public interface AmazonS3PoolListener {

	public void doAddObject(AmazonS3Pool redisPool) throws AmazonS3PoolListenerException;

	public void doBorrowObject(AmazonS3Pool redisPool, long borrowMaxWaitMillis) throws AmazonS3PoolListenerException;

	public void doClear(AmazonS3Pool redisPool);

	public void doClose(AmazonS3Pool redisPool);

	public void doEvict(AmazonS3Pool redisPool) throws AmazonS3PoolListenerException;

	public void returnObject(AmazonS3Pool redisPool, AmazonS3 as3);
}
