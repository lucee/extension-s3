package org.lucee.extension.resource.s3.pool;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;

import com.amazonaws.services.s3.AmazonS3;

public class AmazonS3Pool extends GenericObjectPool<AmazonS3> {

	private AmazonS3PoolListener listener;
	private AmazonS3PoolConfig config;

	public AmazonS3Pool(PooledObjectFactory<AmazonS3> factory, AmazonS3PoolConfig config, AmazonS3PoolListener listener) {
		super(factory, config);
		this.listener = listener;
		this.config = config;
	}

	@Override
	public void addObject() throws Exception {
		if (listener != null) listener.doAddObject(this);
		super.addObject();
	}

	@Override
	public AmazonS3 borrowObject() throws Exception {
		return borrowObject(-1);
	}

	@Override
	public AmazonS3 borrowObject(long borrowMaxWaitMillis) throws Exception {
		if (listener != null) listener.doBorrowObject(this, borrowMaxWaitMillis);
		return super.borrowObject(borrowMaxWaitMillis);
	}

	@Override
	public void clear() {
		if (listener != null) listener.doClear(this);
		super.clear();
	}

	@Override
	public void close() {
		if (listener != null) listener.doClose(this);
		super.close();
	}

	@Override
	public void evict() throws Exception {
		if (listener != null) listener.doEvict(this);
		super.evict();
	}

	@Override
	public void returnObject(AmazonS3 as3) {
		if (listener != null) listener.returnObject(this, as3);
		super.returnObject(as3);
	}

}
