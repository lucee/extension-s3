package org.lucee.extension.resource.s3;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.lucee.extension.resource.s3.region.RegionFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.analytics.AnalyticsConfiguration;
import com.amazonaws.services.s3.model.intelligenttiering.IntelligentTieringConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.metrics.MetricsConfiguration;
import com.amazonaws.services.s3.model.ownership.OwnershipControls;
import com.amazonaws.services.s3.waiters.AmazonS3Waiters;

import lucee.commons.io.log.Log;

public class AmazonS3Client implements AmazonS3 {

	private static Map<String, AmazonS3Client> pool = new ConcurrentHashMap<String, AmazonS3Client>();

	/*
	 * LDEV-7004: inner clients that are no longer used for new operations (the wrapper expired, or the inner client was
	 * replaced by invalidateAmazonS3). Other threads can still be in the middle of an operation with them, or still read
	 * a response stream that holds one of their pooled connections, so they are not shut down right away but by
	 * shutdownRetired() once their connection pool is idle.
	 */
	private static final ConcurrentLinkedQueue<Retired> retired = new ConcurrentLinkedQueue<Retired>();
	// a retired client is never shut down before this; covers a thread that got the client but has not leased a connection yet
	static final long RETIRED_MIN_AGE = 10_000L;
	// if the state of the connection pool cannot be read, shut down after this
	static final long RETIRED_UNKNOWN_STATE_AGE = 300_000L;
	// shut down after this in any case, even when a connection is still leased (a response stream that is never closed)
	static final long RETIRED_MAX_AGE = 3_600_000L;
	private static final long SHUTDOWN_RETIRED_INTERVAL = 1_000L;
	private static volatile long lastShutdownRetired;
	// LDEV-7004: the state of the connection pool could not be read by reflection, logged once
	private static volatile boolean poolStatsFallbackLogged;

	// private S3 s3;
	private org.lucee.extension.resource.s3.region.RegionFactory.Region region;

	// LDEV-7004: volatile and only replaced under the lock of this instance (see invalidateAmazonS3)
	private volatile AmazonS3 client;
	// LDEV-7004: true once this wrapper was removed from the pool, an inner client it creates after that is retired too
	private volatile boolean removedFromPool;
	private Log log;

	private long created;

	private String accessKeyId;
	private String secretAccessKey;
	private String host;

	private long liveTimeout;

	private boolean pathStyleAccess;
	private S3HttpPoolSettings httpPool;

	public static AmazonS3Client get(String accessKeyId, String secretAccessKey, String host, org.lucee.extension.resource.s3.region.RegionFactory.Region region, long liveTimeout,
			boolean pathStyleAccess, S3HttpPoolSettings httpPool, Log log) throws S3Exception {

		S3HttpPoolSettings poolSettings = httpPool == null ? S3HttpPoolSettings.fromEnv() : httpPool;
		String key = accessKeyId + ":" + secretAccessKey + ":" + host + ":" + (region == null ? "default-region" : S3.toString(region)) + ":" + pathStyleAccess + ":"
				+ poolSettings.toCacheKey();
		AmazonS3Client client = pool.get(key);
		if (client == null || client.isExpired()) {
			synchronized (pool) {
				AmazonS3Client existing = pool.get(key);
				if (existing == null || existing.isExpired()) {
					// LDEV-7004: other threads can still use the expired client, it is shut down once it is idle
					if (existing != null) existing.removeFromPool();
					pool.put(key, client = new AmazonS3Client(accessKeyId, secretAccessKey, host, region, key, liveTimeout, pathStyleAccess, poolSettings, log));
					if (log != null) log.debug("S3", "create client for  [" + accessKeyId + ":...@" + host + "] maxConnections=" + poolSettings.getEffectiveMaxConnections());
				}
				else {
					client = existing;
				}
			}

		}
		shutdownRetired(System.currentTimeMillis(), false);
		return client;
	}

	private AmazonS3Client(String accessKeyId, String secretAccessKey, String host, org.lucee.extension.resource.s3.region.RegionFactory.Region region, String key,
			long liveTimeout, boolean pathStyleAccess, S3HttpPoolSettings httpPool, Log log) throws S3Exception {
		this.accessKeyId = accessKeyId;
		this.secretAccessKey = secretAccessKey;
		this.host = host;
		this.region = region;
		this.pathStyleAccess = pathStyleAccess;
		this.httpPool = httpPool;
		this.log = log;
		this.created = System.currentTimeMillis();
		client = create();
		this.liveTimeout = liveTimeout;
	}

	public AmazonS3 create() throws S3Exception {
		AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
		builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey)));

		ClientConfiguration config = new ClientConfiguration();
		httpPool.apply(config);
		builder.withClientConfiguration(config);

		if (host != null && !host.isEmpty() && !host.equalsIgnoreCase(S3.DEFAULT_HOST)) {
			builder = builder.withEndpointConfiguration(new EndpointConfiguration(host, region == null ? "us-east-1" : S3.toString(region)));
		}
		else {
			if (region != null) {
				builder = builder.withRegion(region.getName());
			}
			else {
				builder = builder.withRegion(RegionFactory.US_EAST_1.getName()).withForceGlobalBucketAccessEnabled(true);
			}
		}
		if (pathStyleAccess) builder.withPathStyleAccessEnabled(pathStyleAccess);

		builder.withRequestHandlers(new S3HttpPoolMonitor(httpPool, log, accessKeyId + ":...@" + host));
		return builder.build();
	}

	public boolean isBackBlaze() {
		return host != null && host.toLowerCase().indexOf(S3.BACKBLAZE.toLowerCase()) != -1;
	}

	public boolean isWasabi() {
		return host != null && host.toLowerCase().indexOf(S3.WASABI.toLowerCase()) != -1;
	}

	public boolean isGoogle() {
		return host != null && host.toLowerCase().indexOf(S3.GOOGLE.toLowerCase()) != -1;
	}

	private boolean isExpired() {
		return liveTimeout > 0 && (System.currentTimeMillis() - created) > liveTimeout;
	}

	@Override
	public void abortMultipartUpload(AbortMultipartUploadRequest amur) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.abortMultipartUpload(amur);
		}
		catch (RuntimeException e) {
			retry(c, e).abortMultipartUpload(amur);
		}
	}

	@Override
	public void changeObjectStorageClass(String arg0, String arg1, StorageClass arg2) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.changeObjectStorageClass(arg0, arg1, arg2);
		}
		catch (RuntimeException e) {
			retry(c, e).changeObjectStorageClass(arg0, arg1, arg2);
		}
	}

	@Override
	public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.completeMultipartUpload(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).completeMultipartUpload(arg0);
		}
	}

	@Override
	public CopyObjectResult copyObject(CopyObjectRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.copyObject(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).copyObject(arg0);
		}
	}

	@Override
	public CopyObjectResult copyObject(String arg0, String arg1, String arg2, String arg3) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.copyObject(arg0, arg1, arg2, arg3);
		}
		catch (RuntimeException e) {
			return retry(c, e).copyObject(arg0, arg1, arg2, arg3);
		}
	}

	@Override
	public CopyPartResult copyPart(CopyPartRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.copyPart(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).copyPart(arg0);
		}
	}

	@Override
	public Bucket createBucket(CreateBucketRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.createBucket(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).createBucket(arg0);
		}
	}

	@Override
	public Bucket createBucket(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.createBucket(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).createBucket(arg0);
		}
	}

	@Override
	public Bucket createBucket(String arg0, Region arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.createBucket(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).createBucket(arg0, arg1);
		}
	}

	@Override
	public Bucket createBucket(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.createBucket(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).createBucket(arg0, arg1);
		}
	}

	@Override
	public void deleteBucket(DeleteBucketRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.deleteBucket(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucket(arg0);
		}
	}

	@Override
	public void deleteBucket(String bucketName) throws SdkClientException, AmazonServiceException {
		DeleteBucketRequest dbr = new DeleteBucketRequest(bucketName);

		AmazonS3 c = client;
		try {
			c.deleteBucket(dbr);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucket(dbr);
		}
	}

	@Override
	public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(DeleteBucketAnalyticsConfigurationRequest arg0)
			throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.deleteBucketAnalyticsConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteBucketAnalyticsConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.deleteBucketAnalyticsConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteBucketAnalyticsConfiguration(arg0, arg1);
		}
	}

	@Override
	public void deleteBucketCrossOriginConfiguration(String arg0) {
		AmazonS3 c = client;
		try {
			c.deleteBucketCrossOriginConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucketCrossOriginConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketCrossOriginConfiguration(DeleteBucketCrossOriginConfigurationRequest arg0) {
		AmazonS3 c = client;
		try {
			c.deleteBucketCrossOriginConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucketCrossOriginConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketEncryptionResult deleteBucketEncryption(String arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.deleteBucketEncryption(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteBucketEncryption(arg0);
		}
	}

	@Override
	public DeleteBucketEncryptionResult deleteBucketEncryption(DeleteBucketEncryptionRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.deleteBucketEncryption(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteBucketEncryption(arg0);
		}
	}

	@Override
	public DeleteBucketIntelligentTieringConfigurationResult deleteBucketIntelligentTieringConfiguration(DeleteBucketIntelligentTieringConfigurationRequest arg0)
			throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.deleteBucketIntelligentTieringConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteBucketIntelligentTieringConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketIntelligentTieringConfigurationResult deleteBucketIntelligentTieringConfiguration(String arg0, String arg1)
			throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.deleteBucketIntelligentTieringConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteBucketIntelligentTieringConfiguration(arg0, arg1);
		}
	}

	@Override
	public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(DeleteBucketInventoryConfigurationRequest arg0)
			throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.deleteBucketInventoryConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteBucketInventoryConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.deleteBucketInventoryConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteBucketInventoryConfiguration(arg0, arg1);
		}
	}

	@Override
	public void deleteBucketLifecycleConfiguration(String arg0) {
		AmazonS3 c = client;
		try {
			c.deleteBucketLifecycleConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucketLifecycleConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationRequest arg0) {
		AmazonS3 c = client;
		try {
			c.deleteBucketLifecycleConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucketLifecycleConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(DeleteBucketMetricsConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.deleteBucketMetricsConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteBucketMetricsConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.deleteBucketMetricsConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteBucketMetricsConfiguration(arg0, arg1);
		}
	}

	@Override
	public DeleteBucketOwnershipControlsResult deleteBucketOwnershipControls(DeleteBucketOwnershipControlsRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.deleteBucketOwnershipControls(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteBucketOwnershipControls(arg0);
		}
	}

	@Override
	public void deleteBucketPolicy(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.deleteBucketPolicy(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucketPolicy(arg0);
		}
	}

	@Override
	public void deleteBucketPolicy(DeleteBucketPolicyRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.deleteBucketPolicy(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucketPolicy(arg0);
		}
	}

	@Override
	public void deleteBucketReplicationConfiguration(String arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			c.deleteBucketReplicationConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucketReplicationConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketReplicationConfiguration(DeleteBucketReplicationConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			c.deleteBucketReplicationConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucketReplicationConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketTaggingConfiguration(String arg0) {
		AmazonS3 c = client;
		try {
			c.deleteBucketTaggingConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucketTaggingConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketTaggingConfiguration(DeleteBucketTaggingConfigurationRequest arg0) {
		AmazonS3 c = client;
		try {
			c.deleteBucketTaggingConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucketTaggingConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketWebsiteConfiguration(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.deleteBucketWebsiteConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucketWebsiteConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketWebsiteConfiguration(DeleteBucketWebsiteConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.deleteBucketWebsiteConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteBucketWebsiteConfiguration(arg0);
		}
	}

	@Override
	public void deleteObject(DeleteObjectRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.deleteObject(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteObject(arg0);
		}
	}

	@Override
	public void deleteObject(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.deleteObject(arg0, arg1);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteObject(arg0, arg1);
		}
	}

	@Override
	public DeleteObjectTaggingResult deleteObjectTagging(DeleteObjectTaggingRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.deleteObjectTagging(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteObjectTagging(arg0);
		}
	}

	@Override
	public DeleteObjectsResult deleteObjects(DeleteObjectsRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.deleteObjects(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).deleteObjects(arg0);
		}
	}

	@Override
	public DeletePublicAccessBlockResult deletePublicAccessBlock(DeletePublicAccessBlockRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.deletePublicAccessBlock(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).deletePublicAccessBlock(arg0);
		}
	}

	@Override
	public void deleteVersion(DeleteVersionRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.deleteVersion(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteVersion(arg0);
		}
	}

	@Override
	public void deleteVersion(String arg0, String arg1, String arg2) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.deleteVersion(arg0, arg1, arg2);
		}
		catch (RuntimeException e) {
			retry(c, e).deleteVersion(arg0, arg1, arg2);
		}
	}

	@Override
	public void disableRequesterPays(String arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			c.disableRequesterPays(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).disableRequesterPays(arg0);
		}
	}

	@Override
	public boolean doesBucketExist(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.doesBucketExist(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).doesBucketExist(arg0);
		}
	}

	@Override
	public boolean doesBucketExistV2(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.doesBucketExistV2(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).doesBucketExistV2(arg0);
		}
	}

	@Override
	public boolean doesObjectExist(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.doesObjectExist(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).doesObjectExist(arg0, arg1);
		}
	}

	@Override
	public PresignedUrlDownloadResult download(PresignedUrlDownloadRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.download(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).download(arg0);
		}
	}

	@Override
	public void download(PresignedUrlDownloadRequest arg0, File arg1) {
		AmazonS3 c = client;
		try {
			c.download(arg0, arg1);
		}
		catch (RuntimeException e) {
			retry(c, e).download(arg0, arg1);
		}
	}

	@Override
	public void enableRequesterPays(String arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			c.enableRequesterPays(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).enableRequesterPays(arg0);
		}
	}

	@Override
	public URL generatePresignedUrl(GeneratePresignedUrlRequest arg0) throws SdkClientException {
		AmazonS3 c = client;
		try {
			return c.generatePresignedUrl(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).generatePresignedUrl(arg0);
		}
	}

	@Override
	public URL generatePresignedUrl(String arg0, String arg1, Date arg2) throws SdkClientException {
		AmazonS3 c = client;
		try {
			return c.generatePresignedUrl(arg0, arg1, arg2);
		}
		catch (RuntimeException e) {
			return retry(c, e).generatePresignedUrl(arg0, arg1, arg2);
		}
	}

	@Override
	public URL generatePresignedUrl(String arg0, String arg1, Date arg2, HttpMethod arg3) throws SdkClientException {
		AmazonS3 c = client;
		try {
			return c.generatePresignedUrl(arg0, arg1, arg2, arg3);
		}
		catch (RuntimeException e) {
			return retry(c, e).generatePresignedUrl(arg0, arg1, arg2, arg3);
		}
	}

	@Override
	public BucketAccelerateConfiguration getBucketAccelerateConfiguration(String arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketAccelerateConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketAccelerateConfiguration(arg0);
		}
	}

	@Override
	public BucketAccelerateConfiguration getBucketAccelerateConfiguration(GetBucketAccelerateConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketAccelerateConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketAccelerateConfiguration(arg0);
		}
	}

	@Override
	public AccessControlList getBucketAcl(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketAcl(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketAcl(arg0);
		}
	}

	@Override
	public AccessControlList getBucketAcl(GetBucketAclRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketAcl(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketAcl(arg0);
		}
	}

	@Override
	public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(GetBucketAnalyticsConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketAnalyticsConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketAnalyticsConfiguration(arg0);
		}
	}

	@Override
	public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketAnalyticsConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketAnalyticsConfiguration(arg0, arg1);
		}
	}

	@Override
	public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String arg0) {
		AmazonS3 c = client;
		try {
			return c.getBucketCrossOriginConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketCrossOriginConfiguration(arg0);
		}
	}

	@Override
	public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(GetBucketCrossOriginConfigurationRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.getBucketCrossOriginConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketCrossOriginConfiguration(arg0);
		}
	}

	@Override
	public GetBucketEncryptionResult getBucketEncryption(String arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketEncryption(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketEncryption(arg0);
		}
	}

	@Override
	public GetBucketEncryptionResult getBucketEncryption(GetBucketEncryptionRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketEncryption(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketEncryption(arg0);
		}
	}

	@Override
	public GetBucketIntelligentTieringConfigurationResult getBucketIntelligentTieringConfiguration(GetBucketIntelligentTieringConfigurationRequest arg0)
			throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketIntelligentTieringConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketIntelligentTieringConfiguration(arg0);
		}
	}

	@Override
	public GetBucketIntelligentTieringConfigurationResult getBucketIntelligentTieringConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketIntelligentTieringConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketIntelligentTieringConfiguration(arg0, arg1);
		}
	}

	@Override
	public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(GetBucketInventoryConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketInventoryConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketInventoryConfiguration(arg0);
		}
	}

	@Override
	public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketInventoryConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketInventoryConfiguration(arg0, arg1);
		}
	}

	@Override
	public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String arg0) {
		AmazonS3 c = client;
		try {
			return c.getBucketLifecycleConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketLifecycleConfiguration(arg0);
		}
	}

	@Override
	public BucketLifecycleConfiguration getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.getBucketLifecycleConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketLifecycleConfiguration(arg0);
		}
	}

	@Override
	public String getBucketLocation(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketLocation(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketLocation(arg0);
		}
	}

	@Override
	public String getBucketLocation(GetBucketLocationRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketLocation(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketLocation(arg0);
		}
	}

	@Override
	public BucketLoggingConfiguration getBucketLoggingConfiguration(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketLoggingConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketLoggingConfiguration(arg0);
		}
	}

	@Override
	public BucketLoggingConfiguration getBucketLoggingConfiguration(GetBucketLoggingConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketLoggingConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketLoggingConfiguration(arg0);
		}
	}

	@Override
	public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(GetBucketMetricsConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketMetricsConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketMetricsConfiguration(arg0);
		}
	}

	@Override
	public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketMetricsConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketMetricsConfiguration(arg0, arg1);
		}
	}

	@Override
	public BucketNotificationConfiguration getBucketNotificationConfiguration(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketNotificationConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketNotificationConfiguration(arg0);
		}
	}

	@Override
	public BucketNotificationConfiguration getBucketNotificationConfiguration(GetBucketNotificationConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketNotificationConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketNotificationConfiguration(arg0);
		}
	}

	@Override
	public GetBucketOwnershipControlsResult getBucketOwnershipControls(GetBucketOwnershipControlsRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketOwnershipControls(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketOwnershipControls(arg0);
		}
	}

	@Override
	public BucketPolicy getBucketPolicy(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketPolicy(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketPolicy(arg0);
		}
	}

	@Override
	public BucketPolicy getBucketPolicy(GetBucketPolicyRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketPolicy(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketPolicy(arg0);
		}
	}

	@Override
	public GetBucketPolicyStatusResult getBucketPolicyStatus(GetBucketPolicyStatusRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.getBucketPolicyStatus(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketPolicyStatus(arg0);
		}
	}

	@Override
	public BucketReplicationConfiguration getBucketReplicationConfiguration(String arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketReplicationConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketReplicationConfiguration(arg0);
		}
	}

	@Override
	public BucketReplicationConfiguration getBucketReplicationConfiguration(GetBucketReplicationConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getBucketReplicationConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketReplicationConfiguration(arg0);
		}
	}

	@Override
	public BucketTaggingConfiguration getBucketTaggingConfiguration(String arg0) {
		AmazonS3 c = client;
		try {
			return c.getBucketTaggingConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketTaggingConfiguration(arg0);
		}
	}

	@Override
	public BucketTaggingConfiguration getBucketTaggingConfiguration(GetBucketTaggingConfigurationRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.getBucketTaggingConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketTaggingConfiguration(arg0);
		}
	}

	@Override
	public BucketVersioningConfiguration getBucketVersioningConfiguration(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketVersioningConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketVersioningConfiguration(arg0);
		}
	}

	@Override
	public BucketVersioningConfiguration getBucketVersioningConfiguration(GetBucketVersioningConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketVersioningConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketVersioningConfiguration(arg0);
		}
	}

	@Override
	public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketWebsiteConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketWebsiteConfiguration(arg0);
		}
	}

	@Override
	public BucketWebsiteConfiguration getBucketWebsiteConfiguration(GetBucketWebsiteConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getBucketWebsiteConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getBucketWebsiteConfiguration(arg0);
		}
	}

	@Override
	public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.getCachedResponseMetadata(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getCachedResponseMetadata(arg0);
		}
	}

	@Override
	public S3Object getObject(GetObjectRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getObject(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObject(arg0);
		}
	}

	@Override
	public S3Object getObject(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getObject(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObject(arg0, arg1);
		}
	}

	@Override
	public ObjectMetadata getObject(GetObjectRequest arg0, File arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getObject(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObject(arg0, arg1);
		}
	}

	@Override
	public AccessControlList getObjectAcl(GetObjectAclRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getObjectAcl(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObjectAcl(arg0);
		}
	}

	@Override
	public AccessControlList getObjectAcl(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getObjectAcl(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObjectAcl(arg0, arg1);
		}
	}

	@Override
	public AccessControlList getObjectAcl(String arg0, String arg1, String arg2) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getObjectAcl(arg0, arg1, arg2);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObjectAcl(arg0, arg1, arg2);
		}
	}

	@Override
	public String getObjectAsString(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.getObjectAsString(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObjectAsString(arg0, arg1);
		}
	}

	@Override
	public GetObjectLegalHoldResult getObjectLegalHold(GetObjectLegalHoldRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.getObjectLegalHold(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObjectLegalHold(arg0);
		}
	}

	@Override
	public GetObjectLockConfigurationResult getObjectLockConfiguration(GetObjectLockConfigurationRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.getObjectLockConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObjectLockConfiguration(arg0);
		}
	}

	@Override
	public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getObjectMetadata(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObjectMetadata(arg0);
		}
	}

	@Override
	public ObjectMetadata getObjectMetadata(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getObjectMetadata(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObjectMetadata(arg0, arg1);
		}
	}

	@Override
	public GetObjectRetentionResult getObjectRetention(GetObjectRetentionRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.getObjectRetention(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObjectRetention(arg0);
		}
	}

	@Override
	public GetObjectTaggingResult getObjectTagging(GetObjectTaggingRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.getObjectTagging(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getObjectTagging(arg0);
		}
	}

	@Override
	public GetPublicAccessBlockResult getPublicAccessBlock(GetPublicAccessBlockRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.getPublicAccessBlock(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getPublicAccessBlock(arg0);
		}
	}

	@Override
	public Region getRegion() {
		AmazonS3 c = client;
		try {
			return c.getRegion();
		}
		catch (RuntimeException e) {
			return retry(c, e).getRegion();
		}
	}

	@Override
	public String getRegionName() {
		AmazonS3 c = client;
		try {
			return c.getRegionName();
		}
		catch (RuntimeException e) {
			return retry(c, e).getRegionName();
		}
	}

	@Override
	public Owner getS3AccountOwner() throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getS3AccountOwner();
		}
		catch (RuntimeException e) {
			return retry(c, e).getS3AccountOwner();
		}
	}

	@Override
	public Owner getS3AccountOwner(GetS3AccountOwnerRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.getS3AccountOwner(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).getS3AccountOwner(arg0);
		}
	}

	@Override
	public URL getUrl(String arg0, String arg1) {
		AmazonS3 c = client;
		try {
			return c.getUrl(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).getUrl(arg0, arg1);
		}
	}

	@Override
	public HeadBucketResult headBucket(HeadBucketRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.headBucket(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).headBucket(arg0);
		}
	}

	@Override
	public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.initiateMultipartUpload(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).initiateMultipartUpload(arg0);
		}
	}

	@Override
	public boolean isRequesterPaysEnabled(String arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.isRequesterPaysEnabled(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).isRequesterPaysEnabled(arg0);
		}
	}

	@Override
	public ListBucketAnalyticsConfigurationsResult listBucketAnalyticsConfigurations(ListBucketAnalyticsConfigurationsRequest arg0)
			throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.listBucketAnalyticsConfigurations(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listBucketAnalyticsConfigurations(arg0);
		}
	}

	@Override
	public ListBucketIntelligentTieringConfigurationsResult listBucketIntelligentTieringConfigurations(ListBucketIntelligentTieringConfigurationsRequest arg0)
			throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.listBucketIntelligentTieringConfigurations(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listBucketIntelligentTieringConfigurations(arg0);
		}
	}

	@Override
	public ListBucketInventoryConfigurationsResult listBucketInventoryConfigurations(ListBucketInventoryConfigurationsRequest arg0)
			throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.listBucketInventoryConfigurations(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listBucketInventoryConfigurations(arg0);
		}
	}

	@Override
	public ListBucketMetricsConfigurationsResult listBucketMetricsConfigurations(ListBucketMetricsConfigurationsRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.listBucketMetricsConfigurations(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listBucketMetricsConfigurations(arg0);
		}
	}

	@Override
	public List<Bucket> listBuckets() throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listBuckets();
		}
		catch (RuntimeException e) {
			return retry(c, e).listBuckets();
		}
	}

	@Override
	public List<Bucket> listBuckets(ListBucketsRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listBuckets(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listBuckets(arg0);
		}
	}

	@Override
	public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listMultipartUploads(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listMultipartUploads(arg0);
		}
	}

	@Override
	public ObjectListing listNextBatchOfObjects(ObjectListing arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listNextBatchOfObjects(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listNextBatchOfObjects(arg0);
		}
	}

	@Override
	public ObjectListing listNextBatchOfObjects(ListNextBatchOfObjectsRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listNextBatchOfObjects(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listNextBatchOfObjects(arg0);
		}
	}

	@Override
	public VersionListing listNextBatchOfVersions(VersionListing arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listNextBatchOfVersions(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listNextBatchOfVersions(arg0);
		}
	}

	@Override
	public VersionListing listNextBatchOfVersions(ListNextBatchOfVersionsRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listNextBatchOfVersions(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listNextBatchOfVersions(arg0);
		}
	}

	@Override
	public ObjectListing listObjects(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listObjects(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listObjects(arg0);
		}
	}

	@Override
	public ObjectListing listObjects(ListObjectsRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listObjects(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listObjects(arg0);
		}
	}

	@Override
	public ObjectListing listObjects(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listObjects(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).listObjects(arg0, arg1);
		}
	}

	@Override
	public ListObjectsV2Result listObjectsV2(String arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listObjectsV2(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listObjectsV2(arg0);
		}
	}

	@Override
	public ListObjectsV2Result listObjectsV2(ListObjectsV2Request arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listObjectsV2(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listObjectsV2(arg0);
		}
	}

	@Override
	public ListObjectsV2Result listObjectsV2(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listObjectsV2(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).listObjectsV2(arg0, arg1);
		}
	}

	@Override
	public PartListing listParts(ListPartsRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listParts(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listParts(arg0);
		}
	}

	@Override
	public VersionListing listVersions(ListVersionsRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listVersions(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).listVersions(arg0);
		}
	}

	@Override
	public VersionListing listVersions(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listVersions(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).listVersions(arg0, arg1);
		}
	}

	@Override
	public VersionListing listVersions(String arg0, String arg1, String arg2, String arg3, String arg4, Integer arg5) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.listVersions(arg0, arg1, arg2, arg3, arg4, arg5);
		}
		catch (RuntimeException e) {
			return retry(c, e).listVersions(arg0, arg1, arg2, arg3, arg4, arg5);
		}
	}

	@Override
	public PutObjectResult putObject(PutObjectRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.putObject(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).putObject(arg0);
		}
	}

	@Override
	public PutObjectResult putObject(String arg0, String arg1, File arg2) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.putObject(arg0, arg1, arg2);
		}
		catch (RuntimeException e) {
			return retry(c, e).putObject(arg0, arg1, arg2);
		}
	}

	@Override
	public PutObjectResult putObject(String arg0, String arg1, String arg2) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.putObject(arg0, arg1, arg2);
		}
		catch (RuntimeException e) {
			return retry(c, e).putObject(arg0, arg1, arg2);
		}
	}

	@Override
	public PutObjectResult putObject(String arg0, String arg1, InputStream arg2, ObjectMetadata arg3) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.putObject(arg0, arg1, arg2, arg3);
		}
		catch (RuntimeException e) {
			return retry(c, e).putObject(arg0, arg1, arg2, arg3);
		}
	}

	@Override
	public void restoreObject(RestoreObjectRequest arg0) throws AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.restoreObject(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).restoreObject(arg0);
		}
	}

	@Override
	public void restoreObject(String arg0, String arg1, int arg2) throws AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.restoreObject(arg0, arg1, arg2);
		}
		catch (RuntimeException e) {
			retry(c, e).restoreObject(arg0, arg1, arg2);
		}
	}

	@Override
	public RestoreObjectResult restoreObjectV2(RestoreObjectRequest arg0) throws AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.restoreObjectV2(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).restoreObjectV2(arg0);
		}
	}

	@Override
	public SelectObjectContentResult selectObjectContent(SelectObjectContentRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.selectObjectContent(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).selectObjectContent(arg0);
		}
	}

	@Override
	public void setBucketAccelerateConfiguration(SetBucketAccelerateConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			c.setBucketAccelerateConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketAccelerateConfiguration(arg0);
		}
	}

	@Override
	public void setBucketAccelerateConfiguration(String arg0, BucketAccelerateConfiguration arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			c.setBucketAccelerateConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketAccelerateConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketAcl(SetBucketAclRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setBucketAcl(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketAcl(arg0);
		}
	}

	@Override
	public void setBucketAcl(String arg0, AccessControlList arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setBucketAcl(arg0, arg1);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketAcl(arg0, arg1);
		}
	}

	@Override
	public void setBucketAcl(String arg0, CannedAccessControlList arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setBucketAcl(arg0, arg1);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketAcl(arg0, arg1);
		}
	}

	@Override
	public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(SetBucketAnalyticsConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.setBucketAnalyticsConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).setBucketAnalyticsConfiguration(arg0);
		}
	}

	@Override
	public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(String arg0, AnalyticsConfiguration arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.setBucketAnalyticsConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).setBucketAnalyticsConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketCrossOriginConfiguration(SetBucketCrossOriginConfigurationRequest arg0) {
		AmazonS3 c = client;
		try {
			c.setBucketCrossOriginConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketCrossOriginConfiguration(arg0);
		}
	}

	@Override
	public void setBucketCrossOriginConfiguration(String arg0, BucketCrossOriginConfiguration arg1) {
		AmazonS3 c = client;
		try {
			c.setBucketCrossOriginConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketCrossOriginConfiguration(arg0, arg1);
		}
	}

	@Override
	public SetBucketEncryptionResult setBucketEncryption(SetBucketEncryptionRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.setBucketEncryption(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).setBucketEncryption(arg0);
		}
	}

	@Override
	public SetBucketIntelligentTieringConfigurationResult setBucketIntelligentTieringConfiguration(SetBucketIntelligentTieringConfigurationRequest arg0)
			throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.setBucketIntelligentTieringConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).setBucketIntelligentTieringConfiguration(arg0);
		}
	}

	@Override
	public SetBucketIntelligentTieringConfigurationResult setBucketIntelligentTieringConfiguration(String arg0, IntelligentTieringConfiguration arg1)
			throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.setBucketIntelligentTieringConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).setBucketIntelligentTieringConfiguration(arg0, arg1);
		}
	}

	@Override
	public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(SetBucketInventoryConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.setBucketInventoryConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).setBucketInventoryConfiguration(arg0);
		}
	}

	@Override
	public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(String arg0, InventoryConfiguration arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.setBucketInventoryConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).setBucketInventoryConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketLifecycleConfiguration(SetBucketLifecycleConfigurationRequest arg0) {
		AmazonS3 c = client;
		try {
			c.setBucketLifecycleConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketLifecycleConfiguration(arg0);
		}
	}

	@Override
	public void setBucketLifecycleConfiguration(String arg0, BucketLifecycleConfiguration arg1) {
		AmazonS3 c = client;
		try {
			c.setBucketLifecycleConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketLifecycleConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setBucketLoggingConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketLoggingConfiguration(arg0);
		}
	}

	@Override
	public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(SetBucketMetricsConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.setBucketMetricsConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).setBucketMetricsConfiguration(arg0);
		}
	}

	@Override
	public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(String arg0, MetricsConfiguration arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.setBucketMetricsConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).setBucketMetricsConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketNotificationConfiguration(SetBucketNotificationConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setBucketNotificationConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketNotificationConfiguration(arg0);
		}
	}

	@Override
	public void setBucketNotificationConfiguration(String arg0, BucketNotificationConfiguration arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setBucketNotificationConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketNotificationConfiguration(arg0, arg1);
		}
	}

	@Override
	public SetBucketOwnershipControlsResult setBucketOwnershipControls(SetBucketOwnershipControlsRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.setBucketOwnershipControls(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).setBucketOwnershipControls(arg0);
		}
	}

	@Override
	public SetBucketOwnershipControlsResult setBucketOwnershipControls(String arg0, OwnershipControls arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			return c.setBucketOwnershipControls(arg0, arg1);
		}
		catch (RuntimeException e) {
			return retry(c, e).setBucketOwnershipControls(arg0, arg1);
		}
	}

	@Override
	public void setBucketPolicy(SetBucketPolicyRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setBucketPolicy(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketPolicy(arg0);
		}
	}

	@Override
	public void setBucketPolicy(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setBucketPolicy(arg0, arg1);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketPolicy(arg0, arg1);
		}
	}

	@Override
	public void setBucketReplicationConfiguration(SetBucketReplicationConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			c.setBucketReplicationConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketReplicationConfiguration(arg0);
		}
	}

	@Override
	public void setBucketReplicationConfiguration(String arg0, BucketReplicationConfiguration arg1) throws AmazonServiceException, SdkClientException {
		AmazonS3 c = client;
		try {
			c.setBucketReplicationConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketReplicationConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketTaggingConfiguration(SetBucketTaggingConfigurationRequest arg0) {
		AmazonS3 c = client;
		try {
			c.setBucketTaggingConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketTaggingConfiguration(arg0);
		}
	}

	@Override
	public void setBucketTaggingConfiguration(String arg0, BucketTaggingConfiguration arg1) {
		AmazonS3 c = client;
		try {
			c.setBucketTaggingConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketTaggingConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketVersioningConfiguration(SetBucketVersioningConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setBucketVersioningConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketVersioningConfiguration(arg0);
		}
	}

	@Override
	public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setBucketWebsiteConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketWebsiteConfiguration(arg0);
		}
	}

	@Override
	public void setBucketWebsiteConfiguration(String arg0, BucketWebsiteConfiguration arg1) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setBucketWebsiteConfiguration(arg0, arg1);
		}
		catch (RuntimeException e) {
			retry(c, e).setBucketWebsiteConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setEndpoint(String arg0) {
		AmazonS3 c = client;
		try {
			c.setEndpoint(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setEndpoint(arg0);
		}
	}

	@Override
	public void setObjectAcl(SetObjectAclRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setObjectAcl(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setObjectAcl(arg0);
		}
	}

	@Override
	public void setObjectAcl(String arg0, String arg1, AccessControlList arg2) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setObjectAcl(arg0, arg1, arg2);
		}
		catch (RuntimeException e) {
			retry(c, e).setObjectAcl(arg0, arg1, arg2);
		}
	}

	@Override
	public void setObjectAcl(String arg0, String arg1, CannedAccessControlList arg2) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setObjectAcl(arg0, arg1, arg2);
		}
		catch (RuntimeException e) {
			retry(c, e).setObjectAcl(arg0, arg1, arg2);
		}
	}

	@Override
	public void setObjectAcl(String arg0, String arg1, String arg2, AccessControlList arg3) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setObjectAcl(arg0, arg1, arg2, arg3);
		}
		catch (RuntimeException e) {
			retry(c, e).setObjectAcl(arg0, arg1, arg2, arg3);
		}
	}

	@Override
	public void setObjectAcl(String arg0, String arg1, String arg2, CannedAccessControlList arg3) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setObjectAcl(arg0, arg1, arg2, arg3);
		}
		catch (RuntimeException e) {
			retry(c, e).setObjectAcl(arg0, arg1, arg2, arg3);
		}
	}

	@Override
	public SetObjectLegalHoldResult setObjectLegalHold(SetObjectLegalHoldRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.setObjectLegalHold(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).setObjectLegalHold(arg0);
		}
	}

	@Override
	public SetObjectLockConfigurationResult setObjectLockConfiguration(SetObjectLockConfigurationRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.setObjectLockConfiguration(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).setObjectLockConfiguration(arg0);
		}
	}

	@Override
	public void setObjectRedirectLocation(String arg0, String arg1, String arg2) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			c.setObjectRedirectLocation(arg0, arg1, arg2);
		}
		catch (RuntimeException e) {
			retry(c, e).setObjectRedirectLocation(arg0, arg1, arg2);
		}
	}

	@Override
	public SetObjectRetentionResult setObjectRetention(SetObjectRetentionRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.setObjectRetention(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).setObjectRetention(arg0);
		}
	}

	@Override
	public SetObjectTaggingResult setObjectTagging(SetObjectTaggingRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.setObjectTagging(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).setObjectTagging(arg0);
		}
	}

	@Override
	public SetPublicAccessBlockResult setPublicAccessBlock(SetPublicAccessBlockRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.setPublicAccessBlock(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).setPublicAccessBlock(arg0);
		}
	}

	@Override
	public void setRegion(com.amazonaws.regions.Region arg0) throws IllegalArgumentException {
		AmazonS3 c = client;
		try {
			c.setRegion(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setRegion(arg0);
		}
	}

	@Override
	public void setRequestPaymentConfiguration(SetRequestPaymentConfigurationRequest arg0) {
		AmazonS3 c = client;
		try {
			c.setRequestPaymentConfiguration(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setRequestPaymentConfiguration(arg0);
		}
	}

	@Override
	public void setS3ClientOptions(S3ClientOptions arg0) {
		AmazonS3 c = client;
		try {
			c.setS3ClientOptions(arg0);
		}
		catch (RuntimeException e) {
			retry(c, e).setS3ClientOptions(arg0);
		}
	}

	@Override
	public void shutdown() {
		AmazonS3 c = client;
		try {
			c.shutdown();
		}
		catch (RuntimeException e) {
			retry(c, e).shutdown();
		}
	}

	@Override
	public PresignedUrlUploadResult upload(PresignedUrlUploadRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.upload(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).upload(arg0);
		}
	}

	@Override
	public UploadPartResult uploadPart(UploadPartRequest arg0) throws SdkClientException, AmazonServiceException {
		AmazonS3 c = client;
		try {
			return c.uploadPart(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).uploadPart(arg0);
		}
	}

	@Override
	public AmazonS3Waiters waiters() {
		AmazonS3 c = client;
		try {
			return c.waiters();
		}
		catch (RuntimeException e) {
			return retry(c, e).waiters();
		}
	}

	@Override
	public WriteGetObjectResponseResult writeGetObjectResponse(WriteGetObjectResponseRequest arg0) {
		AmazonS3 c = client;
		try {
			return c.writeGetObjectResponse(arg0);
		}
		catch (RuntimeException e) {
			return retry(c, e).writeGetObjectResponse(arg0);
		}
	}

	private synchronized void removeFromPool() {
		if (removedFromPool) return;
		removedFromPool = true;
		retire(client, log);
	}

	/**
	 * LDEV-7004: called when an operation with the inner client <code>failed</code> threw an exception. If the
	 * exception shows that the connection pool of that client is shut down, the inner client is replaced (once, by
	 * the first thread that gets here with it) and the client to retry with is returned, otherwise the exception
	 * is thrown.
	 */
	private AmazonS3 retry(AmazonS3 failed, RuntimeException e) {
		if (!isPoolShutdown(e)) throw e;
		if (log != null) {
			// expected when a thread still holds a wrapper that expired and whose retired client was shut down in the
			// meantime, so no warning for that case
			if (removedFromPool) log.debug("S3", "retry with a new client, the client of the expired wrapper was shut down: " + e);
			else log.log(Log.LEVEL_WARN, "S3", e);
		}
		return invalidateAmazonS3(failed);
	}

	/**
	 * true if the exception (or one of its causes) shows that the connection pool of the client was shut down: an
	 * IllegalStateException (as before), "Connection pool shut down" or the RequestAbortedException
	 * "Request execution failed" of HttpClient (failed to lease a connection from the pool), which the AWS SDK
	 * wraps in a SdkClientException.
	 */
	static boolean isPoolShutdown(Throwable t) {
		if (t instanceof IllegalStateException) return true;
		int depth = 0;
		while (t != null && depth++ < 20) {
			String msg = t.getMessage();
			if (msg != null && msg.indexOf("Connection pool shut down") != -1) return true;
			if (t.getClass().getName().endsWith(".RequestAbortedException") && msg != null && msg.indexOf("Request execution failed") != -1) return true;
			if (t.getCause() == t) break;
			t = t.getCause();
		}
		return false;
	}

	/**
	 * LDEV-7004: replaces the inner client, but only if it is still the instance that failed. Threads that failed
	 * with the same instance at the same time wait here and then use the replacement created by the first one, so a
	 * freshly created client is never replaced or shut down. The failed client is retired, not shut down: if the
	 * failure was not a shut down pool, other threads can still use it.
	 */
	private AmazonS3 invalidateAmazonS3(AmazonS3 failed) throws AmazonS3Exception {
		synchronized (this) {
			AmazonS3 current = client;
			if (current != failed && current != null) return current; // already replaced by another thread
			AmazonS3 fresh;
			try {
				fresh = create();
			}
			catch (Exception e) {
				if (log != null) log.error("S3", e);
				throw new AmazonS3Exception("failed to invalidate client", e);
			}
			client = fresh;
			retire(failed, log);
			// this wrapper is no longer in the pool, so nothing else would ever shut down the new client
			if (removedFromPool) retire(fresh, log);
			return fresh;
		}
	}

	private static void retire(AmazonS3 client, Log log) {
		if (client == null) return;
		synchronized (retired) {
			// never queue the same client twice (identity, not equals)
			for (Retired r: retired) {
				if (r.client == client) return;
			}
			retired.add(new Retired(client, System.currentTimeMillis(), log));
		}
	}

	/**
	 * LDEV-7004: shuts down the retired clients that are no longer used: their connection pool has no leased
	 * connection and no thread waiting for one, and they are retired for at least RETIRED_MIN_AGE (or for
	 * RETIRED_MAX_AGE in any case). Runs at most once a second unless forced.
	 * 
	 * @return number of clients shut down
	 */
	static int shutdownRetired(long now, boolean force) {
		if (retired.isEmpty()) return 0;
		if (!force && now - lastShutdownRetired < SHUTDOWN_RETIRED_INTERVAL) return 0;
		int count = 0;
		synchronized (retired) {
			lastShutdownRetired = now;
			Iterator<Retired> it = retired.iterator();
			while (it.hasNext()) {
				Retired r = it.next();
				if (r.canShutdown(now)) {
					it.remove();
					r.shutdownQuietly();
					count++;
				}
			}
		}
		return count;
	}

	/**
	 * number of connections of the client's connection pool that are leased or waited for, -1 if this cannot be read.
	 * The AWS SDK does not expose its connection manager, so it is read by reflection (AmazonWebServiceClient.client
	 * -&gt; AmazonHttpClient.httpClient -&gt; getHttpClientConnectionManager().getTotalStats()).
	 */
	static int busyConnections(AmazonS3 client) {
		return busyConnections(client, null);
	}

	private static int busyConnections(AmazonS3 client, Log log) {
		try {
			Object http = getField(client, "client");
			Object apache = getField(http, "httpClient");
			Object cm = invoke(apache, "getHttpClientConnectionManager");
			Object stats = invoke(cm, "getTotalStats");
			return ((Number) invoke(stats, "getLeased")).intValue() + ((Number) invoke(stats, "getPending")).intValue();
		}
		catch (Exception e) {
			if (log != null && !poolStatsFallbackLogged) {
				poolStatsFallbackLogged = true;
				log.info("S3", "cannot read the connection pool state of a retired S3 client, it is shut down "
						+ (RETIRED_UNKNOWN_STATE_AGE / 1000) + " seconds after it was retired (logged once): " + e);
			}
			return -1;
		}
	}

	private static Object getField(Object obj, String name) throws Exception {
		Class<?> clazz = obj.getClass();
		while (clazz != null) {
			try {
				Field f = clazz.getDeclaredField(name);
				f.setAccessible(true);
				Object val = f.get(obj);
				if (val == null) throw new NullPointerException(name);
				return val;
			}
			catch (NoSuchFieldException nsfe) {
				clazz = clazz.getSuperclass();
			}
		}
		throw new NoSuchFieldException(name);
	}

	private static Object invoke(Object obj, String name) throws Exception {
		Method m = obj.getClass().getMethod(name);
		m.setAccessible(true);
		return m.invoke(obj);
	}

	private static final class Retired {
		private final AmazonS3 client;
		private final long since;
		private final Log log;

		private Retired(AmazonS3 client, long since, Log log) {
			this.client = client;
			this.since = since;
			this.log = log;
		}

		private boolean canShutdown(long now) {
			long age = now - since;
			if (age >= RETIRED_MAX_AGE) return true;
			if (age < RETIRED_MIN_AGE) return false;
			int busy = busyConnections(client, log);
			if (busy == -1) return age >= RETIRED_UNKNOWN_STATE_AGE;
			return busy == 0;
		}

		private void shutdownQuietly() {
			try {
				client.shutdown();
			}
			catch (Exception e) {
				if (log != null) log.error("S3", e);
			}
		}
	}

	public void release() throws S3Exception {
		// FUTURE remove method
	}

}
