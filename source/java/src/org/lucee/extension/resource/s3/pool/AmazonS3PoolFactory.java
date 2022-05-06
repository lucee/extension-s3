package org.lucee.extension.resource.s3.pool;

import java.io.IOException;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.lucee.extension.resource.s3.S3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import lucee.commons.io.log.Log;

public class AmazonS3PoolFactory extends BasePooledObjectFactory<AmazonS3> {
	private final String host;
	private final String accessKeyId;
	private final String secretAccessKey;
	private final Regions region;
	private final long idleTimeout;
	private final long liveTimeout;
	private final Log log;

	public AmazonS3PoolFactory(String host, String accessKeyId, String secretAccessKey, Regions region, long idleTimeout, long liveTimeout, Log log) {
		this.host = host;
		this.accessKeyId = accessKeyId;
		this.secretAccessKey = secretAccessKey;
		this.region = region;
		this.idleTimeout = idleTimeout;
		this.liveTimeout = liveTimeout;
		this.log = log;
	}

	@Override
	public AmazonS3 create() throws IOException {
		AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
		builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey)));
		// region or endpoint and region
		if (host != null && !host.isEmpty() && !host.equalsIgnoreCase(S3.DEFAULT_HOST)) {
			// TODO serviceEndpoint - the service endpoint either with or without the protocol (e.g.
			// https://sns.us-west-1.amazonaws.com or sns.us-west-1.amazonaws.com)

			builder = builder.withEndpointConfiguration(new EndpointConfiguration(host, region == null ? "us-east-1" : S3.toString(region)));
		}
		else {
			if (region != null) {
				builder = builder.withRegion(region);
			}
			else {
				builder = builder.withRegion(Regions.US_EAST_1).withForceGlobalBucketAccessEnabled(true); // The first region to try your request against
				// If a bucket is in a different region, try again in the correct region

			}
		}
		return builder.build();
	}

	/**
	 * Use the default PooledObject implementation.
	 */
	@Override
	public PooledObject<AmazonS3> wrap(AmazonS3 as3) {
		return new AmazonS3PooledObject(as3);
	}

	@Override
	public boolean validateObject(PooledObject<AmazonS3> po) {
		AmazonS3PooledObject apo = (AmazonS3PooledObject) po;
		// check timeout
		long now = System.currentTimeMillis();
		if (liveTimeout > 0 && apo.created + liveTimeout < now) {
			if (log != null) log.debug("s3", "validateObject(reached live timeout:" + liveTimeout + ") " + host + ":" + region);
			return false;
		}

		if (idleTimeout > 0 && apo.lastAccess + idleTimeout < now) {
			if (log != null) log.debug("s3", "validateObject(reached idle timeout:" + idleTimeout + ") " + host + ":" + region);
			return false;
		}
		// TODO better way to verify
		/*
		 * AmazonS3 as3 = apo.getObject();
		 * 
		 * try { long start = System.currentTimeMillis(); as3.doesBucketExistV2("sssssssssssssss");
		 * print.e("time validate:" + (System.currentTimeMillis() - start)); } catch (Exception e) {
		 * print.e(e); return false; }
		 */

		return true;
	}

	@Override
	public void passivateObject(PooledObject<AmazonS3> po) throws Exception {
		AmazonS3PooledObject apo = (AmazonS3PooledObject) po;

		if (log != null) log.debug("s3", "passivateObject");
		apo.lastAccess = System.currentTimeMillis();
		super.passivateObject(apo);
	}

	@Override
	public void destroyObject(PooledObject<AmazonS3> po) throws Exception {
		AmazonS3 as3 = po.getObject();
		if (as3 != null) {
			if (log != null) log.debug("s3", "destroyObject() " + host + ":" + region);
			as3.shutdown();
		}
	}
}
